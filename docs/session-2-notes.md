1. **What is the bundle.name set to?** :`bundle.name` is set to dab_training
2. **How many targets are defined? What are their names?** : two targets are defined - dev and prod.
3. **What is mode: development doing?** : it prefixes any deployed assets with dev, so that they can be differentiated even if production assets are deployed in the same workspace.
4. **What resource files are included via the include directive?** : Currently a sample_job.yml. In general, it includes jobs, pipelines, clusters etc.
5. **What is in the src/ folder?** : It contains the source code.
6. **What should be added to .gitignore?** : Anything files / file types that need not be commited to the git/github repository.