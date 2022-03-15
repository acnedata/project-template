# *A*CNE *D*ata *C*ookie*c*utter: Projects

This is a [cookiecutter](https://cookiecutter.readthedocs.io/) template used to set up a fresh data project at ACNE
Data.


## Project structure
```
YYYY-MM-client-project    <- The top-level repository in the stated naming convention.
│                            (e.g. 2022-03-acne-survey-analysis).
│
├── .dvc                  <- Configuration files for DVC.
│   ├── .gitignore
│   └── config
├── .gitignore            <- Set up with sensible defaults for excluding files.
│
├── .pre-commit.yaml      <- Configuration file for pre-commit.
│
├── CHANGELOG.md          <- Document to keep track of changes made to the project.
│
├── README.md             <- The top-level README with a brief description of this project.
│
├── data                  <- Here all data is stored which is being version controlled by DVC.
│   │
│   ├── interim           <- Data generated in intermediate cleaning and transformation steps.
│   │
│   ├── processed         <- The final, fully processed data used for traning and visualizations.
│   │
│   └── raw               <- The original, immutable data.
│
├── notebooks             <- Jupyter notebooks used for data exploration and code experimentation.
│                            Naming convention is MMdd-initials-description. (e.g. 0311-iw-data exploration)
│
├── pyproject.toml        <- Here are all project dependencies listed. Some dev dependencies are already defined.
│
├── src                   <- Source code used in this project.
│   └── __init__.py
└── tests                 <- Code for pytest.
    └── __init__.py
```

## Dependencies

To be able to properly work with this template some lodal dependencies need to be fullfilled. Namely AWS CLI,
GitHub CLI, Git, DVC, cruft, and Poetry need to be installed. 


## Initialization

1. Move into the directory where you want to create a new project and run:
```bash
cruft create https://github.com/acnedata/adcc-projects
```

2. You will be prompted to enter some information about this project such as the project name. 

Then `cd` into this new directory and initialize it with:
```
git init && dvc init && dvc install
```

3. Then install the Git hooks that syncronize some of the Git and DVC commands.
```
dvc install
```

4. Next a repository on GitHub as to be created with the follwing command (changing placeholders of course):
```
gh repo create --private acnedata/<YYYY-MM-client-project>
```

5. And then this repository to be added as remote to the local project (it is recommended to use this https-syntax):
```
git remote add origin https://github.com/acnedata/<YYYY-MM-client-project>
```

6. We next install all dev-dependencies with Poetry.
```
poetry install
```

7. And add code-formatting pre-commit hooks.
```
pre-commit install
```


## Picking up a in-progress project

Is as easy as cloning the repository stored on GitHub. `cd` to the folder where you store your local repositories
(such as `~/github/`), and run (changing placeholders):
```
git clone https://github.com/acnedata/YYYY-MM-client-project> .
```


## Usage

### Git & DVC

It is assumed that you have a basic understanding of how to work with `Git`, how to `add`, `commit`, `push`, `pull`, etc.
DVC follows a very similar model.

When new data files are added or generated, we can track them with `dvc add <path-to-file>`. Similary, changes to data
files are logged with `dvc commit`.

Thanks to the Git hooks, `dvc checkout` on `git checkout` and `dvc push` on `git push` are fully automated. We are also
being on `git commit` if `dvc commit` needs to be run.

### Poetry

Poetry and the accompanying `pyproject.toml` are our drop-in replacement for pip and `requirements.txt`. 

Usage is simple. We can specify packages need for our project with `poetry add <pypi-package-name>`. Please check the
[Poetry documentation](https://python-poetry.org/docs/dependency-specification/) for how to specify version constraints.

The dependencies are then installed by running `poetry install`.


## Updating the template in an ongoing project

As updates to the template are being made such as addition or removal of code checking tools, we want to propagte those
updates to onging projects as well.

To update an existing project with template changes, just run the follwing in the root of the project:
```
cruft update
```


## Documentation

- [AWS CLI](https://docs.aws.amazon.com/cli/index.html)
- [cruft](https://cruft.github.io/cruft/)
- [DVC](https://dvc.org/doc)
- [Git](https://git-scm.com/doc)
- [GitHub CLI](https://cli.github.com/manual/index)
- [Poetry](https://python-poetry.org/docs/)
