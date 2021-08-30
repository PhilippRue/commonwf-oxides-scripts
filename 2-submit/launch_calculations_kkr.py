import time

#from re import S
from aiida.plugins import DataFactory, WorkflowFactory
from aiida import orm
from aiida.engine import submit

from aiida_common_workflows.common import ElectronicType, RelaxType, SpinType
from aiida_common_workflows.plugins import get_entry_point_name_from_class
from aiida_common_workflows.plugins import load_workflow_entry_point
from aiida_submission_controller import FromGroupSubmissionController

DRY_RUN = False
MAX_CONCURRENT = 30
PLUGIN_NAME = 'kkr'
CODE_LABEL = 'KKRhost@claix18_sshtunnel'
CODE_PREPARE_LABEL = 'voronoi@localhost'

STRUCTURES_GROUP_LABEL = f'commonwf-oxides/set1/structures/{PLUGIN_NAME}'
WORKFLOWS_GROUP_LABEL = f'commonwf-oxides/set1/workflows/{PLUGIN_NAME}'

class EosSubmissionController(FromGroupSubmissionController):
    """A SubmissionController for submitting EOS with Quantum ESPRESSO common workflows."""
    def __init__(self, code_label, *args, **kwargs):
        """Pass also a code label, that should be a code associated to an `quantumespresso.pw` plugin."""
        super().__init__(*args, **kwargs)
        self._code = orm.load_code(code_label)
        self._process_class = WorkflowFactory('common_workflows.eos')

    def get_extra_unique_keys(self):
        """Return a tuple of the keys of the unique extras that will be used to uniquely identify your workchains.

        Here: the chemical symbol of the element, and the configuration (XO, XO2, X2O3, ...).
        """
        return ['element', 'configuration']

    def get_inputs_and_processclass_from_extras(self, extras_values):
        """Return inputs and process class for the submission of this specific process.

        I just submit an ArithmeticAdd calculation summing the two values stored in the extras:
        ``left_operand + right_operand``.
        """
        structure = self.get_parent_node_from_extras(extras_values)

        sub_process_cls = load_workflow_entry_point('relax', PLUGIN_NAME)
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


        generator.get_builder(structure, engines)

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

        return inputs, self._process_class

if __name__ == "__main__":
    controller = EosSubmissionController(
        parent_group_label=STRUCTURES_GROUP_LABEL,
        code_label=CODE_LABEL,
        group_label=WORKFLOWS_GROUP_LABEL,
        max_concurrent=MAX_CONCURRENT)
    
    print('Already run    :', controller.num_already_run)
    print('Max concurrent :', controller.max_concurrent)
    print('Available slots:', controller.num_available_slots)
    print('Active slots   :', controller.num_active_slots)
    print('Still to run   :', controller.num_to_run)
    print()

    run_processes = controller.submit_new_batch(dry_run=DRY_RUN)
    for run_process_extras, run_process in run_processes.items():
        if run_process is None:
            print(f'{run_process_extras} --> To be run')    
        else:
            print(f'{run_process_extras} --> PK = {run_process.pk}')

    print()
