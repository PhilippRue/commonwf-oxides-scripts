from aiida.plugins import DataFactory, WorkflowFactory
from aiida import orm
from aiida.engine import submit

from aiida_common_workflows.common import ElectronicType, RelaxType, SpinType
from aiida_common_workflows.plugins import get_entry_point_name_from_class
from aiida_common_workflows.plugins import load_workflow_entry_point

PLUGIN_NAME = 'kkr'
CODE_LABEL = 'KKRhost@claix18_sshtunnel'
CODE_PREPARE_LABEL = 'voronoi@localhost'

STRUCTURES_GROUP_LABEL = f'commonwf-oxides/set1/structures/{PLUGIN_NAME}'
WORKFLOWS_GROUP_LABEL = f'commonwf-oxides/set1/workflows/{PLUGIN_NAME}'

Structure = DataFactory('structure')

query = orm.QueryBuilder()
query.append(Structure, tag='structure', project=['extras', '*'])
query.append(orm.Group, tag='group', filters={'label': STRUCTURES_GROUP_LABEL}, with_node='structure')
all_structures = {(res[0]['element'], res[0]['configuration']): res[1] for res in query.all()}

structure = all_structures[('Al', 'X2O')]
print(f'Structure PK: {structure.pk}')

sub_process_cls = load_workflow_entry_point('relax', 'kkr')
sub_process_cls_name = get_entry_point_name_from_class(sub_process_cls).name
generator = sub_process_cls.get_input_generator()

engine_types = generator.get_engine_types()
engines = {}
engines['relax'] = {
    'code': CODE_LABEL,
     'options': {
        'withmpi': True,
        'resources': {
            'num_machines': 2,
            'tot_num_mpiprocs': 48*2,
        },
        'max_wallclock_seconds': 3600*10,
        'custom_scheduler_commands': '#SBATCH --account=jara0191\n\nulimit -s unlimited; export OMP_STACKSIZE=2g; export OMP_NUM_THREADS=1;',
    },
}
engines['voronoi'] = {
    'code': CODE_PREPARE_LABEL,
     'options': {
        'withmpi': False,
        'resources': {
            'num_machines': 1,
        },
        'max_wallclock_seconds': 600,
    },
}

inputs = {
    'structure': structure,
    'generator_inputs': {  # code-agnostic inputs for the relaxation
        'engines': engines,
        'protocol': 'precise',
        'relax_type': RelaxType.NONE,
        'electronic_type': ElectronicType.METAL,
        'spin_type': SpinType.NONE,
    },
    'sub_process_class': sub_process_cls_name,
    'sub_process' : {  # optional code-dependent overrides
    }
}

cls = WorkflowFactory('common_workflows.eos')
submit(cls, **inputs)
