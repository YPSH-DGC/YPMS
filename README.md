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
If you have already installed the PyYPSH runtime, running `ypsh --ypms` will install the launcher.
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
Contributions to YPMS itself are also welcome.
You can contribute via issue or PR.

## Custom YPMS Repository
Creating a YPMS repository is easy.
Simply create a file structure based on the `yopr` directory in this repository and make it accessible via HTTPS.
*Some file customizations may be required.
