import argparse
import solv
import sys

from pulp.plugins.loader import manager
from pulp.server.db.model import RepositoryContentUnit
from pulp.server.db.model.criteria import Criteria
from pulp.server.db.connection import initialize as db_initialize


class AttributeFactory(object):
    def __init__(self, attr_name, set_none=False, do_str=True):
        """Declare a simple attribute.

        Usage:

        pool = solv.Pool()
        repo = solv.add_repo('Foo')
        solvable = repo.add_solvable()
        name_attribute = AttributeFactory('name')
        name_attribute(foo, lion_rpm)

        assert foo.name == lion_rpm.name

        This is primarily intended to construct a solv solvable from a content
        unit which requires:
        * no attribute set instead an attribute value of None
        * any attribute has to be put thru str()

        To use on generic objects rather than on solvables, setting
        the attributes can be adjusted by the
        set_none=False and do_str=True keywords.

        :param attr_name: attribute name to be set on the target object
        :type attr_name: basestring
        :param set_none: a flag to control setting None as attribute value
        :type set_none: True/False
        :param do_str: a flag whether to apply str() to a value before setting
                       the attribute
        :type do_str: True/False
        """

        self.attr_name = attr_name
        self.set_none = set_none
        self.do_str = do_str

    def __call__(self, solvable, unit, parent_factory=None):
        """Set the solvable.<attr_name> with the unit.<attr_name> value.

        It might be required later on to be able to use a different target
        attribute name; maybe with a target_attr_name=None keyword.


        :param solvable: a solv solvable object
        :type source: a solv solvable object
        :param unit: a content unit or a dictionary to get the <attr_name>
                     value from
        :type unit: an object or a dictionary
        :param parent_factory: ignored
        :returns: None
        """
        if isinstance(unit, dict):
            value = unit.get(self.attr_name)
        else:
            value = getattr(unit, self.attr_name)
        print('processing unit {} attribute {} value {}'.format(
            unit, self.attr_name, value))
        if value is None and not self.set_none:
            return
        # all solvable attributes have to be basestrings
        setattr(solvable, self.attr_name, self.do_str and str(value) or value)


class EVRAttributeFactory(object):
    """A specific, epoch, version and release compound attribute.

    Would be provided by pulp_rpm unless e.g the Deb plug-in requires this
    """
    attribute_factory = AttributeFactory('evr')
    attribute_factories = [
        AttributeFactory('epoch', set_none=True),
        AttributeFactory('version', set_none=True),
        AttributeFactory('release', set_none=True),
    ]

    @staticmethod
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

    class Adaptor(object):
        @property
        def evr(self):
            return EVRAttributeFactory.format_evr(
                self.version, self.epoch, self.release)

    def __call__(self, solvable, unit, *args):
        """Set the solvable evr attribute.

        :param solvable: the sovlable to set the attribute of
        :type sovlable: an object
        :param unit: the unit to use to get the composing attribute values from
        :type unit: an object or a dictionary
        :returns: None
        """
        # create a fresh adaptor instance
        adaptor = self.Adaptor()

        # pull the compound values out from the unit object, pretending
        # the adaptor is a solvable
        for attribute_factory in self.attribute_factories:
            attribute_factory(adaptor, unit, *args)

        # set the solvable.evr with the value of the adaptor
        # NOTE: by default the adaptor is str()ed before assigning
        self.attribute_factory(solvable, adaptor, *args)


class RpmDependencyAttributeFactory(object):
    attribute_factories = [
        AttributeFactory('name', set_none=True),
        EVRAttributeFactory(),
        AttributeFactory('flags', set_none=True, do_str=False),
    ]

    class Adaptor(object):
        pass

    def __init__(self, attr_name):
        """An RPM-specific dependency factory.

        for the provides, requires, etc... dependency attributes
        nested, generic factory that creates the solv.Rel/solv.Dep
        objects and registers those to a solvable

        :param attr_name: the attribute name to use
        :type attr_name: basestring
        """
        self.attr_name = attr_name

    def __call__(self, solvable, unit, solv_factory):
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
        :param solv_factory: the solvable factory
        :type solv_factory: BasetUnitSolvableFactory
        :returns: None
        """
        # e.g SOLVABLE_PROVIDES, SOLVABLE_REQUIRES...
        keyname = getattr(solv, 'SOLVABLE_{}'.format(self.attr_name.upper()))
        # process all the records in e.g unit.requires which is a list of
        # dictionaries describing the unit dependencies
        dependency_unit_infos = getattr(unit, self.attr_name, [])
        print('processing unit {} attribute {} value {}'.format(
            unit, self.attr_name, dependency_unit_infos))
        pool = solv_factory.solv_repo.pool
        for depinfo in dependency_unit_infos:
            adaptor = self.Adaptor()
            for attribute_factory in self.attribute_factories:
                attribute_factory(adaptor, depinfo, solv_factory)
            if adaptor.name.startswith('('):
                # the Rich/Boolean dependencies have just the 'name' attribute
                # this is always in the form: '(foo >= 1.2 AND bar != 0.9)'
                dep = pool.parserpmrichdep(adaptor.name)
            else:
                # generic dependencies provide at least a solvable name
                dep = pool.Dep(adaptor.name)
                if adaptor.flags:
                    # in case the flags unit attribute is populated, use it as
                    # a solv.Rel object to denote solvable--dependency
                    # relationship dependency in this case is a relationship
                    # towards the dependency made from the 'flags', e.g:
                    # solv.REL_AND, and the evr fields
                    dep = dep.Rel(
                        getattr(solv, 'REL_{}'.format(adaptor.flags)),
                        pool.Dep(adaptor.evr)
                    )
            # register the constructed solvable dependency
            solvable.add_deparray(keyname, dep)


class BasetUnitSolvableFactory(object):
    attribute_factories = []

    def __init__(self, solv_repo):
        # a solv.id <-> content_unit.id mapping
        # might use DBM to relieve memory pressure
        self.id_mapping = {}
        self.solv_repo = solv_repo

    def __call__(self, unit):
        solvable = self.solv_repo.add_solvable()
        for attribute_factory in self.attribute_factories:
            attribute_factory(solvable, unit, self)
        self.register(solvable, unit)
        return solvable

    def register(self, solvable, unit):
        if isinstance(unit, dict):
            # avoid mapping dependency dicts
            return
        self.id_mapping[unit.id] = solvable.id
        # FIXME the unit is cached this might exhaust memory
        # good only for demo purposes
        self.id_mapping[solvable.id] = unit

    def get_unit(self, solvable_id):
        return self.id_mapping.get(solvable_id)

    def get_solvable(self, unit_id):
        return self.id_mapping.get(unit_id)


class RpmUnitSolvableFactory(BasetUnitSolvableFactory):
    # An RPM content unit nests dependencies in a dict format
    # Would be provided by pulp_rpm
    attribute_factories = [
        AttributeFactory('name'),
        EVRAttributeFactory(),
        AttributeFactory('arch'),
        AttributeFactory('vendor'),
        RpmDependencyAttributeFactory('requires'),
        RpmDependencyAttributeFactory('conflicts'),
        RpmDependencyAttributeFactory('provides'),
        RpmDependencyAttributeFactory('obsoletes'),
        RpmDependencyAttributeFactory('recommends'),
        RpmDependencyAttributeFactory('suggests'),
        RpmDependencyAttributeFactory('supplements'),
        RpmDependencyAttributeFactory('enhances'),
    ]


def load_repo_units(plugin_manager, repo_name, factory):
    for rcu in RepositoryContentUnit.objects.find_by_criteria(
            Criteria(filters={'repo_id': repo_name})):
        # just the RPMs for now O:-)
        if rcu.unit_type_id != 'rpm':
            print('skipping {}'.format(rcu.unit_type_id))
            continue
        model = plugin_manager.unit_models[rcu.unit_type_id]
        unit = model.objects.get(pk=rcu.unit_id)
        print('loaded {}'.format(unit))
        factory(unit)


if __name__ == '__main__':

    argparser = argparse.ArgumentParser()
    argparser.add_argument('--source-repo', default='zoo')
    argparser.add_argument('--unit', default='penguin')
    argparser.add_argument('--target-repo', default='zoo')
    argparser.add_argument('--ignore-recommends', action='store_true')
    args = argparser.parse_args()

    pm = manager.PluginManager()
    db_initialize()

    pool = solv.Pool()
    pool.setarch()
    # pretend nothing has been installed so far
    target_repo = pool.add_repo('@System')
    t_rufus = RpmUnitSolvableFactory(target_repo)
    load_repo_units(pm, args.target_repo, t_rufus)
    pool.installed = target_repo

    # load the Pulp repo provided on the CLI
    solv_repo = pool.add_repo(args.source_repo)
    s_rufus = RpmUnitSolvableFactory(solv_repo)
    load_repo_units(pm, args.source_repo, s_rufus)

    for solvable in pool.solvables:
        print("{}".format(solvable))

    # ###
    # Cargo-culting https://github.com/openSUSE/libsolv/blob/master/examples/pysolv
    #
    pool.createwhatprovides()

    # lookup is provided by libsolv; mind the SOLVER_SOLVABLE_NAME flag
    # to list the dependencies an installation is pretended
    # If this was a recursive copy task, the target repo would have been used
    # as libsolv system repo to prevent copying dependencies already satisfied
    # in the target repo.
    job = pool.Job(solv.Job.SOLVER_SOLVABLE_NAME |
                   solv.Job.SOLVER_INSTALL, pool.str2id(args.unit))

    solver = pool.Solver()
    solver.set_flag(solv.Solver.SOLVER_FLAG_IGNORE_RECOMMENDED, args.ignore_recommends)

    problems = solver.solve([job])

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
        print('solvable - {} as unit: {}'.format(
            s, s_rufus.get_unit(s.id)))


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

    print('Done.')
