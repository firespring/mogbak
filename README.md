Mogbak makes it easy to backup your MogileFS domain to a single self contained directory.  It has the ability to    
update that directory again and again to match your MogileFS domain.  This makes it possible for you to use   
LVM snapshots.  Mogbak can also fork worker processes to backup or restore files in parallel.

##Need a backup?
    mogbak create --db=mogilefs --dbhost=mysqlserver --dbpass=secret --dbuser=mogile --domain=awesomeapp \\
    --trackerip=10.10.10.10 --workers=10 /backups/awesomeapp
    mogbak backup /backups/awesomeapp
    
###Perhaps you want an incremental, no problem.
    mogbak backup --workers=10 /backups/awesomeapp
    
###Maybe you need to see the files in the backup
    mogbak list /backups/awesomeapp
    
###Backups suck if you can't restore,  well good thing we can
    mogbak restore --domain=restoreawesomeapp --trackerip=10.10.10.10 --workers=10 /backups/awesomeapp
    
###Maybe you just want to restore one file?
    mogbak restore --domain=restoreawesomeapp --trackerip=10.10.10.10 --single-file=abc1234file --workers=10 /backups/awesomeapp


###Why does Mogbak need to connect to my database?
MogileFS simply bumps its FID value in the files table when a new file is saved. This makes it quite simple
for us to query and see what files need to be backed up since our last backup. The problem is that we also need
to know what files have been deleted from MogileFS but still live within your backup.  Since MogileFS has no delete
log for us to look at we need to query the database in a brute-force manner. This would be extremely painful without
access to the database. We do this as efficiently as we can,  our cluster has about 3 million files and it takes less than a second.
You can disable this feature with --no-delete switch.

The good news is that mogbak only needs *SELECT access*.

###What does the self contained backup directory look like?

 * db.sqlite - holds the metadata of each file in the backup
 * settings.yml - holds the settings to connect to the mysql database and the tracker
 * Backup files hashed using the same scheme as MogileFS Server

###Whats the catch?

 * Space. Obviously with large clusters the ability to save a full backup onto one device probably isn't possible
 * Database.  Right now only MySQL backed trackers are supported

There are certainly things that could be done about the above issues.  Pull requests are welcome :)

####Requirements

 * Ruby 1.9 is what we test against.  It'll probably work under 1.8 but you'll be a ghiney pig.
 * *nix
 * mysql client development libraries (for mysql2 gem dependency)
 * sqlite3 development libraries (for sqlite3 gem dependency)

####How to install?
    gem install mogbak

####There is more,  check out the full syntax for all the features
See https://github.com/firespring/mogbak/wiki/Command-syntax