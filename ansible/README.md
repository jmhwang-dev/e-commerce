python3 -m pip install --user pipx
python3 -m pipx ensurepath
pipx install --include-deps ansible

<!-- ansible-playbook playbook.yml --private-key /path/ssh_key -->