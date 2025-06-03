# DisableACTier2
The second part. Automate the locking of terminated employees' accounts in the active directory. Script PowerShell .PS1
The script under consideration accesses a particular OU and looks up all user accounts. 
It then removes all groups in the user's Active Directory settings with the exception of the Domain Users group.
