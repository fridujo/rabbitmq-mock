# Contributing to RabbitMQ-mock

:+1::tada: First off, thanks for taking the time to contribute! :tada::+1:

## How Can I Contribute?
Know that I will try my best to help you, whatever path you choose

### Reporting Bugs
Using *proper English*, explain the problem and include additional details like links to the official documentation of RabbitMQ.  
Then describe a reproducible scenario, that can be directly turned into a **runnable test**.  
Better, provide some code, inlined in the issue or in a dedicated repository showing the abnormal behavior.

*In fine*, the fix **will** be shipped with tests proving that it works (~better than before).

### Suggesting a new Feature
The sole purpose of this project is to ease the life of Java-ish (understand, any language on the JVM) developpers working with RabbitMQ.  
So start to explain how this feature will improve the developer experience (reducing boiler-code, speeding-up feedback loop, etc.).  
If there is a new API involved, please submit your idea about it, whatever language (JVM based maybe ?).

### Pull Requests
When writing a PR, please take the time to read the existing code, and follow the implicit formatting rules (brace on the same line than **if** statement, this kind of stuff).  
Plus, supply one or more tests covering the production code changes.  
Avoid if possible, mutable structures (especially unused-and-without-control *setters*), this projects serves in multi-threaded contexts and it must be modified avoiding race-conditions or non-deterministic behaviors.  
Last but not least, take good care of your Git history, PR are integrated using **Rebase And Merge**, only the commits you push will be kept (no squashing, no merge commit).  
If you new to Git, start by reading these [guidelines](https://chris.beams.io/posts/git-commit/).
