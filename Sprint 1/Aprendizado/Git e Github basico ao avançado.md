## Branches

check existing a branches
```bash
git branch
```

creating a new branch
```bash
git branch <branch name>

#Example
git branch subscription-page
```

delete a branch (not recommended)
```bash
git branch -d <branch name>
#or
git branch --delete <branch name>

#Example
git branch -d subscription-page
```

change working branch, this command bring uncommitted changes to the other branch, needs caution.
```bash
git checkout <branch name>

#Create the branch relative to working branch and checkout
git checkout -b <branch name>

#Example
git branch -b subscribe-form
```

branch union
```bash
#Bring branch modifications to working branch
git merge <branch name>
```

branch stash
```bash
#send all changes of a branch into a stash
git stash

#check created stashs
git stash list

#Retrieve a stash
git stash apply <stash name>

#Removing all branch stashs
git stash remove clear

#Remove specific stash
git stash drop <stash name>
```

tags, a checkpoints for a branch 
```bash
#Create a tag
git tag -a <tag name> -m "<msg>"

#Check created tags
git tag

#Show details of a tag
git show <tag name>

#Change working tag
git checkout <tag name>

#Remove specific stash
git stash drop <stash name>

#Pushing tags to repository
git push origin <tag name>

#Push all tags to repository
git push origin --tags
```

## Update and Share Repositories
finding branches
```bash
#Update the list of mapped branches
git fetch
```

receiving udpates
```bash
#Retrieving updates of working branch from other repos
git pull

#Retrieving specific branch (not recomended)
git pull origin <branch name>
```

sending changes
```bash
#Sending updates of working branch to other repos
git push

#Sending to specific branch (not recomended)
git push origin <branch name>
```

working with remote repositories
```bash
#Check all remote repositories added
git remote [-v | --verbose]

#Add a remote repository
git remote add <name> <URL>

#Rename an added origin
git remote rename <old name> <new name>

#Remove added remote repository
git remote remove <name>
```

### Submodules

A way to have two or more projects in the same repository, a repository inside a repository
```bash
#Check all existent submodules
git submodule

#Add another repo as a submodule
git submodule add <repository>

#Push changes in a submodule
git push --recurse-submodules=on-demand
```

## Analysis and Inspection of Repositories
showing import information
```bash
#Show a lot of useful information about the wornking branch
#informations about realiazed commits and modifications
#between them 
git show

#Show information of tags
git show <tag>
```

checking diferences
```bash
git diff <file_a> <file_b>
#or
git diff <branch_a> <branch_b>
```

get a short log of changes in project
```bash
git shortlog
```

## Repository Management

Cleaning untracked changes
```bash
git clean

#For interactive remove
git clean -i
```

repository otimization
```bash
#Use git garbage collector
git gc
```

checking files integrity
```bash
git fsck
```

Check all steps of a repository
```bash
#Get all information from the last 30 days
git reflog
```

Zipping a repository
```bash
git archive --format zip --ouput <filename>.zip <branch>
```

## Improving Project Commits

Private branches
```bash
#Passing commits of private branch to a working branch
#interactively
git rebase <private_branch> <working_branch> -i
#Inside the document you can change the commits to:
#pick: use commmit
#rework: rename commmit
#squash: discard commit 
```
#### Clean commits:
* Separate **subject** from **message**
* Max 50 characters per subject
* Subject initial letter in uppercase
* Max 72 characters for message body
* Explain why and how of commmit