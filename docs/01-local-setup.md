# Local environment setup

Step-by-step setup of a fresh developer machine for this project.

Reference machine: **Ubuntu 22.04.4 LTS**.

## Step 1 — Base system packages

Requires `sudo`.

```bash
sudo apt update
sudo apt install -y git curl build-essential openjdk-17-jdk-headless
```

| Package | Why |
|---|---|
| `git` | Version control. |
| `curl` | Downloads the installers for `uv` and the Databricks CLI. |
| `build-essential` | C compiler toolchain. Some Python packages compile native code on install. |
| `openjdk-17-jdk-headless` | JVM required by local PySpark: Spark runs on the JVM, Python only builds the execution plan. Java 17 is supported by Spark 3.5 and 4.x. |

### Verify

```bash
git --version     # git version 2.34.1  
curl --version    # curl 7.81.0
gcc --version     # gcc 11.4.0
java -version     # openjdk version "17.0.20.1"
```

## Step 2 — Git identity

Configure the commit author once per machine. Use the GitHub **noreply** email so the personal email is never exposed in public commits (GitHub → *Settings → Emails*).

```bash
git config --global user.name "Your Name"
git config --global user.email "<id>+<github-user>@users.noreply.github.com"
git config --global init.defaultBranch main
```

### Verify

```bash
git config --global --list
# user.name=Your Name
# user.email=<id>+<github-user>@users.noreply.github.com
# init.defaultbranch=main
```

## Step 3 — uv (Python and dependency manager)

[uv](https://docs.astral.sh/uv/) replaces `pyenv`, `venv`, `pip`, `pip-tools`/`poetry` and `pipx` with a single tool. It pins the Python version per project (`.python-version`) and every dependency (`uv.lock`), so the laptop, CI and Databricks run the same versions. The system Python (3.10 on Ubuntu 22.04) is left untouched.

No `sudo` needed: it installs into `~/.local/bin`.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env   # or open a new terminal
```

### Verify

```bash
uv --version    # uv 0.13.7
```

## Step 4 — Databricks CLI

The [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/) is a single Go binary used to authenticate, deploy Asset Bundles, upload files to Volumes and call the workspace API. Requires `sudo` (installs into `/usr/local/bin`).

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
```

### Authenticate (OAuth, user-to-machine)

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

This opens the browser, asks for a profile name and writes it to `~/.databrickscfg`. With `auth_storage = secure`, OAuth tokens are kept in the OS keyring, not in the file. No personal access token (PAT) is needed.

### Verify

```bash
databricks --version        # Databricks CLI v1.23.0
databricks auth profiles    # profile listed with Valid = YES
databricks current-user me  # your user
databricks catalogs list    # system, samples, workspace
```

## Step 5 — GitHub CLI and SSH key

> Do not use `apt install gh` from the Ubuntu repositories: on 22.04 it ships gh 2.4.0 (2022), which lacks flags and commands used here. Install from GitHub's official apt repository instead.

Requires `sudo`.

```bash
sudo mkdir -p -m 755 /etc/apt/keyrings
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
  | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg > /dev/null
sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
  | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null

sudo apt update
sudo apt install -y gh
```

### Authenticate and create the SSH key

```bash
gh auth login --insecure-storage
```

| Prompt | Answer |
|---|---|
| Where do you use GitHub? | `GitHub.com` |
| Preferred protocol for Git operations? | `SSH` |
| Generate a new SSH key? | `Yes` (passphrase optional, title e.g. `ubuntu-laptop`) |
| How would you like to authenticate? | `Login with a web browser` |

Copy the one-time code, then open `https://github.com/login/device` in a browser already signed in to the right account. `gh` uploads the SSH public key to GitHub for you.

`--insecure-storage` stores the gh token in `~/.config/gh/hosts.yml` (readable only by your user) instead of the system keyring, which avoids keyring unlock prompts.

> Use an **authenticator app** plus saved **recovery codes** for GitHub 2FA. A passkey alone, stored in a password manager behind a PIN you don't know, can lock you out of the account permanently.

### Verify

```bash
gh --version              # gh version 2.103.3
gh auth status            # Logged in to github.com account <user>, protocol: ssh
ssh -T git@github.com     # Hi <user>! You've successfully authenticated...
```

## Step 6 — Project environment

Tools from steps 1–5 are installed system-wide or per user. Python **libraries** live only in the project's virtual environment (`.venv/`), which `uv` creates and manages. Never `pip install` into the system Python: Ubuntu depends on it.

```bash
git clone git@github.com:<github-user>/<repo>.git
cd <repo>
uv sync          # downloads Python 3.12 if needed, creates .venv/ from uv.lock
uv run pytest    # first run downloads the Delta Lake jars (~1 min)
```

| File | Role |
|---|---|
| `.python-version` | Python version (3.12, matching Databricks serverless environment 6) |
| `pyproject.toml` | Declared dependencies and tool config (pytest, ruff) |
| `uv.lock` | Exact resolved versions. Committed. |
| `.venv/` | The virtual environment. Not committed, rebuilt with `uv sync`. |

There's no need to `activate` the environment: `uv run <cmd>` always uses `.venv/`.

### Verify

```bash
uv run pytest                     # 2 passed (Spark + Delta smoke tests)
databricks bundle validate -t dev # Validation OK!
```
