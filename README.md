
<img width="1920" alt="ypsh_full_black" src="https://github.com/user-attachments/assets/39d0a722-3cec-47bc-8b10-6fd1cddde3bb" />

> [!IMPORTANT]
> I am developing various projects, and since I manage, maintain, and update all of them myself, there may be times when I can't attend to each project individually.
> For example, large-scale projects like [Zeta-LLM](https://github.com/Zeta-DGC/Zeta-LLM) are very difficult to develop.
> [PyYPSH](https://github.com/YPSH-DGC/YPSH) is also a challenging project, as it implements a custom programming language using ASTs, which is quite advanced.
> If you would like to report bugs or suggest new features for my projects, I would greatly appreciate it if you could use pull requests and make them ready to merge, if possible.
> Also, if someone else has already created an issue, I would be thankful if you could create a pull request that immediately addresses the problem, if you're able to.
> (This message is displayed in some repositories created by Nercone. Translated by GPT-4o.)

# YPMS / YOPR
YPSH Package Management Script and YPSH Official Package Repository

## What is YPMS?
YPMS is a package manager created for YPSH.<br>
Of course, it can also be used for purposes other than YPSH.

## Installation

> [!WARNING]
> YPMS is not binary-based and relies on shebang.<br>
> Therefore, it does not work in some environments (most Windows environments).<br>
> We plan to address this in the future.<br>
> (If you have a workaround, please help us by submitting a pull request.)

### Installation using PyYPSH
**Prerequisite**
- Python 3.x (Recommend 3.12+)
- YPSH v8.1+

If you have already installed the PyYPSH runtime, running `ypsh --ypms` will install the launcher.<br>
After that, you can use `ypms` as is. [^1]

[^1]: The `ypms` command is immediately available only if the default installation path of the ypsh binary has been added to your PATH environment variable. If not, you can run it using an absolute or relative path.

## Contributing

### Package Submission
If you'd like to add a new package, please send us a pull request.<br>
There are almost no restrictions on the type of package.<br>
You can publish a library for YPSH as a package, or even your own CLI tool.<br>
However, harmful packages may be rejected.<br>
Also, since YOPR is an official repository, it requires a certain level of recognition or expectation.<br>
You can also set up your own YPMS repository.<br>
In that case, see [Custom YPMS Repository].

### Contributing to YPMS Itself
Contributions to YPMS itself are also welcome.<br>
You can contribute via issue or PR.

## Custom YPMS Repository
Creating a YPMS repository is easy.<br>
Simply create a file structure based on the `yopr` directory in this repository and make it accessible via HTTPS.<br>
*Some file customizations may be required.
