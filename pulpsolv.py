import argparse
import collections
import functools
import solv
import sys

from pulp.plugins.loader import manager
from pulp.server.db.model import RepositoryContentUnit
from pulp.server.db.model.criteria import Criteria
from pulp.server.db.connection import initialize as db_initialize


def attribute_factory(attr_name, set_none=False, conversion=lambda x: str(x),
                      target_attr=None, default=None):
    """Declare a simple attribute.

    Usage:

    pool = solv.Pool()
    repo = solv.add_repo('Foo')
    solvable = repo.add_solvable()
    name_attribute = attribute_factory('name')
    name_attribute(foo, lion_rpm)

    assert foo.name == lion_rpm.name

    This is primarily intended to construct a solv solvable from a content
    unit which requires:
    * no attribute set instead an attribute value of None
    * any attribute has to be put thru str()

    To use on generic objects rather than on solvables, setting
    the attributes can be adjusted by the
    set_none=False and conversion keywords.

    :param attr_name: attribute name to be set on the target object
    :type attr_name: basestring
    :param set_none: a flag to control setting None as attribute value
    :type set_none: True/False
    :param conversion: a function converting the value from the unit space
                        to the solvable space
    :type conversion: callable(value) -> value or None
    :param target_attr: the target attribute to use
    :type target_attr: basestring or None
    :param default: default value to use.
    :type default: object or None
    """

    def inner_factory(solvable, unit, repo=None):
        """Set the solvable.<attr_name> with the unit.<attr_name> value.

        :param solvable: a solv solvable object
        :type solvable: a solv solvable object
        :param unit: a content unit or a dictionary to get the <attr_name>
                     value from
        :type unit: an object or a dictionary
        :param repo: the repo being populated; ignored
        :type repo: solv.Repo
        :returns: None
        """
        if isinstance(unit, dict):
            value = unit.get(attr_name, default)
        else:
            value = getattr(unit, attr_name, default)
        if conversion:
            value = conversion(value)
        print('processing unit {} attribute {} value {} {}'.format(
            unit, attr_name, value,
            'as: {}'.format(target_attr) if target_attr else ''))
        if value is None and not set_none:
            return
        setattr(solvable, target_attr or attr_name, value)
    return inner_factory


def format_evr(version, epoch=None, release=None):
    """The EVR value has a specific way of representation in solv.

    :param version: the version value
    :type version: basestring
    :param epoch: the epoch value; optional
    :type epoch: basestring or None
    :param release: the release value; optional
    :type release: basestring or None
    :returns: the epoch:version-release string
    """
    return '{}{}{}'.format(
            '{}:'.format(epoch) if epoch else '',
            version,
            '-{}'.format(release) if release else ''
        )


def compound_attribute_factory(adaptor_factory, *compound_factories):
    def outer(fn):
        @functools.wraps(fn)
        def inner(solvable, unit, repo=None):
            adaptor = adaptor_factory()
            for attr_factory in compound_factories:
                attr_factory(adaptor, unit, repo=repo)
            fn(solvable, adaptor, repo=repo)
        return inner
    return outer


def evr_attribute_factory(attr_name='evr'):
    """A specific, epoch, version and release compound attribute."""

    class Adaptor(object):
        @property
        def evr(self):
            return format_evr(self.version, self.epoch, self.release)

    return compound_attribute_factory(
        Adaptor,
        attribute_factory('epoch', set_none=True, conversion=None),
        attribute_factory('version', set_none=True, conversion=None),
        attribute_factory('release', set_none=True, conversion=None),
    )(attribute_factory('evr'))


def multi_attribute_factory(attr_name):
    def outer(fn):
        @functools.wraps(fn)
        def inner(solvable, unit, repo=None):
            for item in getattr(unit, attr_name, []):
                fn(solvable, item, repo=repo)
        return inner
    return outer


def rpm_dependency_attribute_factory(attr_name, dependency_key=None):
    """An RPM-specific dependency factory.

    for the provides, requires, etc... dependency attributes
    nested, generic factory that creates the solv.Rel/solv.Dep
    objects and registers those to a solvable

    :param attr_name: the attribute name to use
    :type attr_name: basestring
    :param dependency_key: the key to use e.g solv.SOLVABLE_REQUIRES
    :type dependency_key: solv.SOLVABLE_REQUIRES/_PROVIDES... or None
    """
    class Adaptor(object):
        pass

    @multi_attribute_factory(attr_name)
    @compound_attribute_factory(
        Adaptor,
        attribute_factory('name'),
        evr_attribute_factory(),
        attribute_factory('flags', set_none=True, conversion=None)
    )
    def inner_factory(solvable, unit, repo=None):
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
        :param repo: the solv repo being populated
        :type repo: solv.Repo
        :returns: None
        """
        # e.g SOLVABLE_PROVIDES, SOLVABLE_REQUIRES...
        keyname = dependency_key or getattr(solv, 'SOLVABLE_{}'.format(attr_name.upper()))
        pool = repo.pool
        if unit.name.startswith('('):
            # the Rich/Boolean dependencies have just the 'name' attribute
            # this is always in the form: '(foo >= 1.2 AND bar != 0.9)'
            dep = pool.parserpmrichdep(unit.name)
        else:
            # generic dependencies provide at least a solvable name
            dep = pool.Dep(unit.name)
            if unit.flags:
                # in case the flags unit attribute is populated, use it as
                # a solv.Rel object to denote solvable--dependency
                # relationship dependency in this case is a relationship
                # towards the dependency made from the 'flags', e.g:
                # solv.REL_AND, and the evr fields
                dep = dep.Rel(
                    getattr(solv, 'REL_{}'.format(unit.flags)),
                    pool.Dep(unit.evr)
                )
            # register the constructed solvable dependency
            solvable.add_deparray(keyname, dep)
    return inner_factory


def unit_solvable_converter_factory(*attribute_factories):
    """Create a factory of a content unit--solv.Solvable converter.

    Each attribute factory either calls setattr on a solvable with a converted attribute value
    or processes the attribute value and changes the state of either the solvable being created
    or the solv.Repo or both by e.g adding dependencies.

    :param attribute_factories: the attribute factories to use for the conversion
    :type attribute_factories: a list of (solvable, unit, repo=None) -> None callables
    :returns: a (solv_repo, unit) -> solv.Solvable callable
    :rtype: callable
    """
    def unit_solvable_converter(solv_repo, unit):
        """Convert a unit into a solv.Solvable.

        As an inevitable side effect, the solvable is added into the repo specified.

        :param solv_repo: the repository the unit is being added into
        :type solv_repo: solv.Repo
        :param unit: the unit being converted
        :type unit: pulp_rpm.plugins.models.Model
        :return: the solvable created.
        :rtype: solv.Solvable
        """
        solvable = solv_repo.add_solvable()
        for attribute_factory in attribute_factories:
            attribute_factory(solvable, unit, solv_repo)
        return solvable
    return unit_solvable_converter


rpm_unit_solvable_factory = unit_solvable_converter_factory(
    # An RPM content unit nests dependencies in a dict format
    # Would be provided by pulp_rpm
    attribute_factory('name'),
    evr_attribute_factory(),
    attribute_factory('arch'),
    attribute_factory('vendor'),
    rpm_dependency_attribute_factory('requires'),
    rpm_dependency_attribute_factory('conflicts'),
    rpm_dependency_attribute_factory('provides'),
    rpm_dependency_attribute_factory('obsoletes'),
    rpm_dependency_attribute_factory('recommends'),
    rpm_dependency_attribute_factory('suggests'),
    rpm_dependency_attribute_factory('supplements'),
    rpm_dependency_attribute_factory('enhances'),
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
    attribute_factory('errata_id', target_attr='name',
                      conversion=lambda x: 'errata:{}'.format(x)),
    attribute_factory('arch', default='noarch'),
    attribute_factory('errata_from', target_attr='vendor'),
    evr_attribute_factory(),
    # FIXME: not all these units are really required; what pulp does is it
    # filters rpm_search_dicts to nevras actually present in the source repo:
    #   https://github.com/pulp/pulp_rpm/blob/ef5fc5b2af47736114b68bc08658d9b2a94b84e1/plugins/pulp_rpm/plugins/importers/yum/associate.py#L91,#L94
    # Could this be handled as some ignore_missing?
    rpm_dependency_attribute_factory(
        'rpm_search_dicts', dependency_key=solv.SOLVABLE_REQUIRES),
))


srpm_sovable_factory = unit_solvable_converter_factory(
    # An SRPM content unit factory
    attribute_factory('name'),
    evr_attribute_factory(),
    attribute_factory('arch'),
    attribute_factory('vendor'),
    rpm_dependency_attribute_factory('requires'),
    rpm_dependency_attribute_factory('conflicts'),
)

MODEL_SOLVABLE_FACTORY_MAPPING = {
    'rpm': rpm_unit_solvable_factory,
    'erratum': erratum_solvable_factory,
    'srpm': srpm_sovable_factory,
}


def load_repo_units(plugin_manager, repo, repo_name, mapping):
    for rcu in RepositoryContentUnit.objects.find_by_criteria(
            Criteria(filters={'repo_id': repo_name})):
        try:
            factory = MODEL_SOLVABLE_FACTORY_MAPPING[rcu.unit_type_id]
        except KeyError:
            print('skipping {}'.format(rcu.unit_type_id))
            continue
        model = plugin_manager.unit_models[rcu.unit_type_id]
        unit = model.objects.get(pk=rcu.unit_id)
        solvable = factory(repo, unit)

        mapping.setdefault(solvable.id, unit)
        mapping.setdefault(unit.id, solvable)

        print('loaded {}'.format(unit))


PoolMapping = collections.namedtuple('PoolMapping', 'pool mapping')


def pool_mapping_factory(plugin_manager, source_repo_name, target_repo_name=None):
    poolmapping = PoolMapping(solv.Pool(), {})
    # prevent https://github.com/openSUSE/libsolv/issues/267
    poolmapping.pool.setarch()
    source_repo = poolmapping.pool.add_repo(source_repo_name)
    load_repo_units(plugin_manager, source_repo, source_repo_name, poolmapping.mapping)

    if not target_repo_name:
        return poolmapping

    target_repo = poolmapping.pool.add_repo(target_repo_name)
    load_repo_units(plugin_manager, target_repo, target_repo_name, poolmapping.mapping)
    poolmapping.pool.installed = target_repo
    return poolmapping


if __name__ == '__main__':

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--source-repo', default='zoo')
    argparser.add_argument('--unit', default='penguin')
    argparser.add_argument('--target-repo', default='zoo')
    argparser.add_argument('--ignore-recommends', action='store_true')
    argparser.add_argument('--debuglevel', choices=[0, 1, 2, 3], type=int,
                           default=0)
    args = argparser.parse_args()

    pm = manager.PluginManager()
    db_initialize()

    poolmapping = pool_mapping_factory(pm, args.source_repo, args.target_repo)
    poolmapping.pool.set_debuglevel(args.debuglevel)

    print('---')
    print('Loaded solvables:')
    for solvable in poolmapping.pool.solvables:
        print("{}".format(solvable))

    print('---')
    print('solving...')
    # ###
    # Cargo-culting https://github.com/openSUSE/libsolv/blob/master/examples/pysolv
    #
    poolmapping.pool.createwhatprovides()

    # lookup is provided by libsolv; mind the SOLVER_SOLVABLE_NAME flag
    # to list the dependencies an installation is pretended
    # If this was a recursive copy task, the target repo would have been used
    # as libsolv system repo to prevent copying dependencies already satisfied
    # in the target repo.

    flags = (
        solv.Selection.SELECTION_NAME |
        solv.Selection.SELECTION_PROVIDES |
        solv.Selection.SELECTION_GLOB |
        solv.Selection.SELECTION_DOTARCH |
        solv.Selection.SELECTION_WITH_SOURCE
    )

    selection = poolmapping.pool.select(args.unit, flags)
    if selection.isempty():
        print("{} not found".format(args.unit))
        sys.exit(1)

    jobs = selection.jobs(solv.Job.SOLVER_INSTALL)

    print('job: {}'.format(jobs))

    solver = poolmapping.pool.Solver()
    solver.set_flag(solv.Solver.SOLVER_FLAG_IGNORE_RECOMMENDED, args.ignore_recommends)

    problems = solver.solve(jobs)

    for problem in problems:
        # problems were encountered resolving the unit dependencies
        print('Found problem: {}'.format(problem))

    if problems:
        sys.exit(1)

    transaction = solver.transaction()

    print('---')
    print('Copying unit "{}" from repo "{}" to repo "{}" requires:'.format(
        args.unit, args.source_repo, args.target_repo))
    for s in transaction.newsolvables():
        print('solvable - {} as unit: {}'.format(s, poolmapping.mapping.get(s.id)))

    print('\nTransaction details:')
    for cl in transaction.classify(
            solv.Transaction.SOLVER_TRANSACTION_SHOW_OBSOLETES |
            solv.Transaction.SOLVER_TRANSACTION_OBSOLETE_IS_UPGRADE):
        print('classified {}'.format(cl))

        for p in cl.solvables():
            if cl.type == (
                    solv.Transaction.SOLVER_TRANSACTION_UPGRADED or
                    cl.type == solv.Transaction.SOLVER_TRANSACTION_DOWNGRADED):
                op = transaction.othersolvable(p)
                print("  - %s -> %s" % (p, op))
            else:
                print("  - %s" % p)

    print ('---')
    print('Alternatives:')
    for alternative in solver.all_alternatives():
        print("{}".format(alternative))
    print('Done.')
