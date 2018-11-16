import argparse
import functools
import logging
import os

import mongoengine
import solv

from pulp_rpm.common import ids
from pulp_rpm.plugins.db import models
from pulp.plugins.loader.api import initialize as plugin_initialize
from pulp.server.controllers import repository as repo_controller
from pulp.server.db.connection import initialize as db_initialize
from pulp.server.db.model import Repository

_LOGGER = logging.getLogger(__name__)


def setattr_conversion(attr_name, set_none=False):
    def outer(conversion):
        def inner(solvable, unit):
            ret = conversion(solvable, unit)
            if not set_none and ret is None:
                return
            setattr(solvable, attr_name, ret)
        return inner
    return outer


def attr_conversion(attr_name, default=None):
    def outer(conversion=lambda sovlable, unit: unit):
        def inner(solvable, unit):
            if isinstance(unit, dict):
                return conversion(solvable, unit.get(attr_name, default))
            else:
                return conversion(solvable, getattr(unit, attr_name, default))
        return inner
    return outer


def multiattr_conversion(*attribute_conversions):
    def outer(conversion):
        def inner(solvable, unit, *args, **kwargs):
            largs = []
            for ac in attribute_conversions:
                largs.append(ac(solvable, unit))
            largs.extend(args)
            return conversion(solvable, *largs, **kwargs)
        return inner
    return outer


def utf8_conversion(conversion=lambda solvable, unit: unit):
    def inner(solvable, unit):
        ret = conversion(solvable, unit)
        if ret is not None:
            return ret.encode('utf-8')
        return ret
    return inner


def repeated_attr_conversion(attribute_conversion):
    def outer(conversion):
        def inner(solvable, unit):
            for value in attribute_conversion(solvable, unit):
                conversion(solvable, value)
        return inner
    return outer


def plain_attribute_factory(attr_name):
    return setattr_conversion(attr_name)(utf8_conversion(attr_conversion(attr_name)()))


@utf8_conversion
@multiattr_conversion(
    attr_conversion('epoch')(),
    attr_conversion('version')(),
    attr_conversion('release')()
)
def evr_unit_conversion(solvable, epoch, version, release):
    if version is None:
        return
    return '{}{}{}'.format(
        '{}:'.format(epoch) if epoch else '',
        version,
        '-{}'.format(release) if release else ''
    )


evr_attribute = setattr_conversion('evr')(evr_unit_conversion)


@multiattr_conversion(
    utf8_conversion(attr_conversion('name')()),
    attr_conversion('flags')(),
    evr_unit_conversion
)
def rpm_dependency_conversion(solvable, unit_name, unit_flags, unit_evr,
                              attr_name, dependency_key=None):
    """Set the solvable dependencies.

    The dependencies of a unit are stored as a list of dictionaries,
    containing following values:
            name: <unit name> or a rich dep string; mandatory
            version: version of the dependency; optional
            epoch: epoch of the dependency; optional
            release: release of the dependency; optional
            flags: AND/OR; optional; if missing meaning by default AND

    These values are parsed by librpm.
    There are two cases how libsolv addresses the dependencies:

    * rich: the name of the dependency contains all required information:
        '(foo >= 1.0-3 AND bar != 0.9)'
        all the other attribute values are ignored

    * generic: the name, version, epoch, release and flags attributes
        are processed explicitly

    The dependency list is either of the provides, requires or the weak
    dependencies, the current case being stored under self.attr_name.

    Libsolv tracks a custom Dep object to represent a dependency of a
    solvable object; these are created in the pool object:

        dependency = pool.Dep('foo')

    The relationship to the solvable is tracked by a Rel pool object:

        relationship = pool.Rel(solv.REL_AND, pool.Dep(evr))

    where the evr is the 'epoch:version-release' string. The relationship
    is then recorded on the solvable explicitly by:

        solvable.add_deparray(solv.SOLVABLE_PROVIDES, relationship)

    If no explict relationship is provided in the flags attribute,
    the dependency can be used directly:

        solvable.add_deparray(solv.SOLVABLE_PROVIDES, dependency)

    :param solvable: a libsolv solvable object
    :type solvable: a libsolv solvable
    :param unit: the content unit to get the dependencies from
    :type unit: an object or a dictionary
    :returns: None
    """
    # e.g SOLVABLE_PROVIDES, SOLVABLE_REQUIRES...
    keyname = dependency_key or getattr(solv, 'SOLVABLE_{}'.format(attr_name.upper()))
    pool = solvable.repo.pool
    if unit_name.startswith('('):
        # the Rich/Boolean dependencies have just the 'name' attribute
        # this is always in the form: '(foo >= 1.2 AND bar != 0.9)'
        dep = pool.parserpmrichdep(unit_name)
    else:
        # generic dependencies provide at least a solvable name
        dep = pool.Dep(unit_name)
        if unit_flags:
            # in case the flags unit attribute is populated, use it as
            # a solv.Rel object to denote solvable--dependency
            # relationship dependency in this case is a relationship
            # towards the dependency made from the 'flags', e.g:
            # solv.REL_EQ, and the evr fields
            if unit_flags == 'EQ':
                rel_flags = solv.REL_EQ
            elif unit_flags == 'LT':
                rel_flags = solv.REL_LT
            elif unit_flags == 'GT':
                rel_flags = solv.REL_GT
            elif unit_flags == 'LE':
                rel_flags = solv.REL_EQ | solv.REL_LT
            elif unit_flags == 'GE':
                rel_flags = solv.REL_EQ | solv.REL_GT
            else:
                # fancier flags; might not be needed actually
                rel_flags = getattr(solv, 'REL_{}'.format(unit_flags))
            dep = dep.Rel(rel_flags, pool.Dep(unit_evr))
        # register the constructed solvable dependency
        solvable.add_deparray(keyname, dep)


def rpm_dependency_attribute_factory(attribute_name, dependency_key=None):
    return repeated_attr_conversion(attr_conversion(attribute_name, default=[])())(
        lambda solvable, unit: rpm_dependency_conversion(
            solvable, unit, attribute_name, dependency_key=dependency_key
        ))


def rpm_filelist_conversion(solvable, unit):
    """A specific, rpm-unit-type filelist attribute conversion."""
    repodata = solvable.repo.first_repodata()
    unit_files = unit.files.get('file')
    if not unit_files:
        return
    for filename in unit_files:
        dirname = os.path.dirname(filename).encode('utf-8')
        dirname_id = repodata.str2dir(dirname, create=True)
        repodata.add_dirstr(solvable.id, solv.SOLVABLE_FILELIST,
                            dirname_id, os.path.basename(filename).encode('utf-8'))


def unit_solvable_converter(solv_repo, unit, *attribute_factories):
    """Create a factory of a content unit--solv.Solvable converter.

    Each attribute factory either calls setattr on a solvable with a converted attribute value
    or processes the attribute value and changes the state of either the solvable being created
    or the solv.Repo or both by e.g adding dependencies.

    :param attribute_factories: the attribute factories to use for the conversion
    :type attribute_factories: a list of (solvable, unit, repo=None) -> None callables
    :param solv_repo: the repository the unit is being added into
    :type solv_repo: solv.Repo
    :param unit: the unit being converted
    :type unit: pulp_rpm.plugins.models.Model
    :return: the solvable created.
    :rtype: solv.Solvable
    """
    solvable = solv_repo.add_solvable()
    for attribute_factory in attribute_factories:
        attribute_factory(solvable, unit)
    return solvable


def unit_solvable_converter_factory(*attribute_factories):
    return lambda solv_repo, unit: unit_solvable_converter(solv_repo, unit, *attribute_factories)


rpm_unit_solvable_factory = unit_solvable_converter_factory(
    # An RPM content unit nests dependencies in a dict format
    # Would be provided by pulp_rpm
    plain_attribute_factory('name'),
    evr_attribute,
    plain_attribute_factory('arch'),
    plain_attribute_factory('vendor'),
    rpm_dependency_attribute_factory('requires'),
    rpm_dependency_attribute_factory('conflicts'),
    rpm_dependency_attribute_factory('provides'),
    rpm_dependency_attribute_factory('obsoletes'),
    rpm_dependency_attribute_factory('recommends'),
    rpm_dependency_attribute_factory('suggests'),
    rpm_dependency_attribute_factory('supplements'),
    rpm_dependency_attribute_factory('enhances'),
    rpm_filelist_conversion,
)


def nonproviding_sovlable_factory(rel_attr_name='evr'):
    """A decorator wrapping a non-providing unit factory with a 'provides' logic.

    E.g an erratum doesn't provide itself as a "dependency".
    The wrapper creates a new solv.SOLVABLE_PROVIDES dependency in the repo/pool
    with the solvable as a target value.

    If the optional rel_attr_name param is provided, a sovl.REL_EQ relationship is created,
    from the attribute value as an EVR equivalent, that refines the dependency.

    :param rel_attr_name: the attribute to use as an evr equivalent when providing
    :type rel_attr_name: basestring
    :returns: a wrapper that ensures a solvable provides itself in the pool
    :rtype: a (solv.Repo, unit) -> solv.Solvable callable
    """
    def outer(fn):
        @functools.wraps(fn)
        def inner(solv_repo, unit):
            solvable = fn(solv_repo, unit)
            dep = solv_repo.pool.Dep(solvable.name)
            rel = None
            if rel_attr_name:
                rel = getattr(solvable, rel_attr_name, None)
            if rel:
                dep = dep.Rel(solv.REL_EQ, solv_repo.pool.Dep(rel))
            solvable.add_deparray(solv.SOLVABLE_PROVIDES, dep)
            return solvable
        return inner
    return outer


erratum_solvable_factory = nonproviding_sovlable_factory('evr')(unit_solvable_converter_factory(
    # cargo-culting from
    # https://github.com/openSUSE/libsolv/blob/master/ext/repo_updateinfoxml.c
    setattr_conversion('name')(utf8_conversion(attr_conversion('errata_id')())),
    setattr_conversion('arch')(lambda solvable, unit: 'noarch'),
    setattr_conversion('vendor')(utf8_conversion(attr_conversion('errata_from')())),
    evr_attribute,
    # filters rpm_search_dicts to nevras actually present in the source repo:
    #   https://github.com/pulp/pulp_rpm/blob/ef5fc5b2af47736114b68bc08658d9b2a94b84e1/plugins/pulp_rpm/plugins/importers/yum/associate.py#L91,#L94
    # this is implemented as solv.SOLVABLE_RECOMMENDS
    rpm_dependency_attribute_factory(
        'rpm_search_dicts', dependency_key=solv.SOLVABLE_RECOMMENDS),
))


srpm_sovable_factory = unit_solvable_converter_factory(
    # An SRPM content unit factory
    plain_attribute_factory('name'),
    evr_attribute,
    plain_attribute_factory('arch'),
    plain_attribute_factory('vendor'),
    rpm_dependency_attribute_factory('requires'),
    rpm_dependency_attribute_factory('conflicts'),
)


class UnitSolvableMapping(object):
    def __init__(self, pool, type_factory_mapping):
        self.type_factory_mapping = type_factory_mapping
        self.pool = pool
        self.mapping = {}
        self.repos = {}

    def _register(self, unit, solvable):
        self.mapping.setdefault(unit.id, solvable)
        self.mapping.setdefault(solvable.id, unit)

    def add_repo_units(self, units, repo_name, installed=False):
        repo = self.repos.get(repo_name)
        if not repo:
            repo = self.repos.setdefault(repo_name, self.pool.add_repo(repo_name))
            repodata = repo.add_repodata()
        else:
            repodata = repo.first_repodata()

        for unit in units:
            try:
                factory = self.type_factory_mapping[unit.type_id]
            except KeyError as err:
                raise ValueError('Unsupported unit type: {}', err)
            solvable = factory(repo, unit)
            self._register(unit, solvable)

        if installed:
            self.pool.installed = repo

        repodata.internalize()

    def get_unit(self, solvable):
        return self.mapping.get(solvable.id)

    def get_solvable(self, unit):
        return self.mapping.get(unit.id)


class Solver(object):
    type_factory_mapping = {
        'rpm': rpm_unit_solvable_factory,
    }
    rpm_fields = list(models.RPM.unit_key_fields) + [
        'provides',
        'requires',
        'version_sort_index',
        'release_sort_index',
        'files',
    ]
    rpm_units_query = functools.partial(
        repo_controller.find_repo_content_units,
        repo_content_unit_q=mongoengine.Q(unit_type_id=ids.TYPE_ID_RPM),
        yield_content_unit=True, unit_fields=rpm_fields
     )

    def __init__(self, source_repo, target_repo=None):
        super(Solver, self).__init__()
        self.source_repo = source_repo
        self.target_repo = target_repo
        self._loaded = False
        self.pool = solv.Pool()
        # prevent https://github.com/openSUSE/libsolv/issues/267
        self.pool.setarch()
        self.mapping = UnitSolvableMapping(
            self.pool, self.type_factory_mapping)

    def load(self):
        if self._loaded:
            return
        self._loaded = True
        repo_name = str(self.source_repo.repo_id)
        self.mapping.add_repo_units(
            self.rpm_units_query(repository=self.source_repo), repo_name)

        if self.target_repo:
            repo_name = str(self.target_repo.repo_id)
            self.mapping.add_repo_units(
                self.rpm_units_query(repository=self.target_repo), repo_name,
                installed=True)

        self.pool.addfileprovides()
        self.pool.createwhatprovides()
        _LOGGER.info('Loaded source repository %s', self.source_repo.repo_id)
        if self.target_repo:
            _LOGGER.info('Loaded target repository %s', self.target_repo.repo_id)

    def _solvable_name_job(self, solvable_name):
        return self.pool.Job(
            solv.Job.SOLVER_SOLVABLE_NAME | solv.Job.SOLVER_INSTALL,
            self.pool.str2id(solvable_name))

    def _units_jobs(self, units):
        for unit in units:
            if isinstance(unit, basestring):
                yield self._solvable_name_job(unit)
                continue

            solvable = self.mapping.get_solvable(unit)
            if not solvable:
                raise ValueError('Encountered an unknown unit {}'.format(unit))
            yield self.pool.Job(solv.Job.SOLVER_INSTALL | solv.Job.SOLVER_SOLVABLE, solvable.id)

    def find_dependent_rpms(self, units):
        solver = self.pool.Solver()

        problems = solver.solve(self._units_jobs(units))
        if problems:
            raise ValueError('Encountered problems solving: {}'.format(
                ", ".join([str(problem) for problem in problems])))

        transaction = solver.transaction()
        return set(self.mapping.get_unit(solvable) for solvable in transaction.newsolvables())


    def optimist(self, solvables, level=0, cache=set()):
        for solvable in solvables:
            if solvable in cache:
                continue
            cache.add(solvable)
            print ("  " * level) + "s: {}".format(solvable)
            yield solvable
            for dep in solvable.lookup_deparray(solv.SOLVABLE_REQUIRES):
                if dep in cache:
                    continue
                cache.add(dep)
                whatprovides = self.pool.whatprovides(dep)
                print ("  " * (level + 1)) + "w {}: {}".format(dep, whatprovides)
                for solvable_ in self.optimist(whatprovides, level=level + 1):
                    yield solvable_

    def brainchild(self, name):
        pool = self.pool
        done = set()
        sel = pool.select(name, solv.Selection.SELECTION_NAME)
        solvables = sel.solvables()
        if not solvables:
            return done
        pkg = sel.solvables()[-1]
        candq = {pkg}
        while candq:
            candq_n = set()
            for cand in candq:
                for req in cand.lookup_deparray(solv.SOLVABLE_REQUIRES):
                    providers = pool.whatprovides(req)
                    if not providers:
                        print("Nothing provides {req!r}".format(req=req))
                        continue
                    for p in pool.whatprovides(req):
                        candq_n.add(p)
                done.add(cand)
            candq = candq_n - done
        return done

    def walk_unit_dependencies(self, units):
        for unit in units:
            dependencies = list(self.optimist(self.pool.whatprovides(self.pool.Dep(unit))))
            print "Dependencies of {} are".format(unit)
            for dependency in dependencies:
                print "  {}".format(dependency)


if __name__ == '__main__':

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--source-repo', default='zoo')
    argparser.add_argument('--unit', dest='units', action='append', default=[])
    argparser.add_argument('--target-repo', nargs='?', default=None)
    argparser.add_argument('--ignore-recommends', action='store_true')
    argparser.add_argument('--debuglevel', choices=[0, 1, 2, 3], type=int,
                           default=0)
    args = argparser.parse_args()

    db_initialize()
    plugin_initialize()

    source_repo = Repository.objects.get(repo_id=args.source_repo)
    target_repo = args.target_repo and Repository.objects.get(repo_id=args.target_repo) or None

    solver = Solver(source_repo, target_repo)
    solver.pool.set_debuglevel(args.debuglevel)
    solver.load()

    print('---')
    print('Query:')
    for unit in args.units:
        print('{}'.format(unit))
        print('  {}'.format(', '.join(str(dep) for dep in solver.brainchild(unit))))

    print
    #print('Dependent units:')
    #for unit in solver.find_dependent_rpms(args.units):
    #    print('{}'.format(unit))
    solver.walk_unit_dependencies(args.units)
