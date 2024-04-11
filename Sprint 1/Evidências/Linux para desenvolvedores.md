## Change permissions of files and directories

- chmod with numeric representation
```bash
chmod xxx file/dir
#xxx = owner, group, others
#0 = ---
#1 = --x
#2 = -w-
#3 = -wx
#4 = r--
#5 = r-x
#6 = rw-
#7 = rwx

chmod 777 t.txt #Everyone has complete access to t.txt
```

- chmod with simbolic representation
```bash
chmod <role><action><permission> file/dir
#roles
#u = owner
#g = group
#o = others
#a = all 3

#action
#+: add permission
#-: remove permission
#=: replace permission

#permissions
#the permissions (r, w, x, wx, rx  etc...)

chmod a=rwx t.txt #Everyone has complete access to t.txt
```

## Zipping and unzipping files
### Tar files
zipping as tar file
```bash
tar  -czvf zipfilename.tar.gz file/dir

#You can add multiple files after zip file name
tar  -czvf zipfilename.tar.gz dir1 file1 dir2 file2 file3
```

unzipping tar file
```bash
tar  -xzvf zipfilename.tar.gz

#You can select where to unzip files
tar  -xzvf zipfilename.tar.gz -c dir/
```

checking whats is inside a tar file
```bash
tar -tvf zipfilename.tar.gz
```

### Zip files
```bash
#To zip a file or dir
zip  -r zipfilename.zip file/dir

#Unzip file
unzip zipfilename.zip -d dir/
```


