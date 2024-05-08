#!/bin/bash

# This checks if the number of arguments is correct
# If the number of arguments is incorrect ( $# != 2) print error message and exit
if [[ $# != 2 ]]
then
  echo "backup.sh target_directory_name destination_directory_name"
  exit
fi

# This checks if argument 1 and argument 2 are valid directory paths
if [[ ! -d $1 ]] || [[ ! -d $2 ]]
then
  echo "Invalid directory path provided"
  exit
fi

# [TASK 1]
targetDirectory=$1
destinationDirectory=$2 

# [TASK 2]
echo "target directory is $1"
echo "destination directory is $2"

# [TASK 3]
currentTS=$(date +%s)

# [TASK 4]
backupFileName="backup-$currentTS.tar.gz"

# We're going to:
  # 1: Go into the target directory
  # 2: Create the backup file
  # 3: Move the backup file to the destination directory

# To make things easier, we will define some useful variables...

# [TASK 5]
origAbsPath=$(pwd)

# [TASK 6]
cd $destinationDirectory
destAbsPath=$destinationDirectory

# [TASK 7]
cd $origAbsPath
cd $targetDirectory

# [TASK 8]
yesterdayTS=$(($currentTS - 24 * 60 * 60))

declare -a toBackup # declares variable called toBackup as an array

for file in $(ls) # [TASK 9]
do
  # [TASK 10]
  if ((`date -r $file +%s` > $yesterdayTS))
  then
    # [TASK 11]
    toBackup+=($file) # add file that was updated in the past 24 hours to the toBackup array
  fi
done

# [TASK 12]
tar -czvf $backupFileName ${toBackup[@]} #  compress and archive the files
# [TASK 13]
mv $backupFileName $destAbsPath 
echo $destAbsPath # Move the file backupFileName to the destination directory located at destAbsPath  
# Congratulations! You completed the final project for this course!
