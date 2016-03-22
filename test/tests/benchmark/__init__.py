from tests.benchmark.utils.ansible_control import ansibleControl

ansible_ctl = ansibleControl()
ansible_ctl.run_playbook('setup_env.yml')
