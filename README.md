# WARNING!!!

DO NOT USE THIS SOFTWARE TO SECURE ANY SORT OF REAL-WORLD COMMUNICATIONS!

This software is for performance testing ONLY! It is full of security vulnerabilities that could be exploited in any real-world deployment.

The purpose of this software is to evaluate the performance of the Riposte system, NOT to be used in a deployment scenario.



## Overview

This code accompanies a paper appearing at the IEEE Symposium
on Security and Privacy ("Oakland") 2015:

  "Riposte: An Anonymous Messaging System Handling Millions of Users"
  Henry Corrigan-Gibbs, Dan Boneh, and David Mazieres

Please direct any questions, comments, or complaints about this
code to henrycg@cs.stanford.edu.



## Repository contents

The code of this repository is split into three branches:

* [`linear`](https://bitbucket.org/henrycg/riposte/src/linear/): Contains the **most modern version of the code**, used in the variant of Riposte that appears in Henry Corrigan-Gibbs' [PhD dissertation](https://purl.stanford.edu/nm483fv2043). This is the cleanest and fastest variant of the scheme and you should use this version unless you have a good reason to prefer the historical ones from the original Riposte paper.
* [`multiparty`](https://bitbucket.org/henrycg/riposte/src/multiparty/): Contains the **three-server Riposte code** used in the original Riposte paper from IEEE S&P 2015. As noted in the [extended version](https://arxiv.org/abs/1503.06115) of the paper, this original protocol has a bug. For the sake of the historical record, the code for the original buggy version that we evaluated is here.
* [`DDH`](https://bitbucket.org/henrycg/riposte/src/DDH/) Contains the **k-server Riposte code** used in the original Riposte paper from IEEE S&P. This variant is much slower, but requires a weaker trust assumption than the three-party variant.

