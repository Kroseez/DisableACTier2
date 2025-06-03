# Import module ActiveDirectory
Import-Module ActiveDirectory

# Function settings
$SearchBase = "ou=,ou=,dc=domain,dc=com" # Your OU with fired accounts
$MaxThreads = 5 # Parallel processing of users, from 1 to 5
$LogFolder = "C:\Logs\ADGroupCleanup"
$LogFile = "$LogFolder\GroupCleanup_$(Get-Date -Format 'yyyyMMdd_HHmmss').log"
$ErrorLogFile = "$LogFolder\GroupCleanup_Errors_$(Get-Date -Format 'yyyyMMdd_HHmmss').csv"

# Create a folder for logs
if (-not (Test-Path $LogFolder)) { 
    New-Item -ItemType Directory -Path $LogFolder | Out-Null 
}

# Let's start logging
Start-Transcript -Path $LogFile -Append
Write-Output "=== Start processing ==="
Write-Output "Date: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Output "OU: $SearchBase"
Write-Output "Max. streams: $MaxThreads"

# Checking access rights
try {
    $TestUser = Get-ADUser -Filter * -SearchBase $SearchBase -ResultSetSize 1 -ErrorAction Stop
    if (-not $TestUser) {
        Write-Output "OU is empty or does not have sufficient permissions."
        Stop-Transcript
        exit
    }
}
catch {
    Write-Output "Error accessing OU: $_"
    Stop-Transcript
    exit
}

# Initializing the error file
Add-Content -Path $ErrorLogFile -Value "Timestamp;User;Group;Error"

# Getting users
try {
    Write-Output "Getting users from $SearchBase..."
    $Users = Get-ADUser -SearchBase $SearchBase -Filter * -ErrorAction Stop | Select-Object -ExpandProperty SamAccountName
}
catch {
    Write-Output "Error getting users: $_"
    Write-Output "Let's try an alternative method..."
    $Users = @()
    $PageSize = 100
    $Offset = 0
    do {
        $UserPage = Get-ADUser -SearchBase $SearchBase -Filter * -ResultPageSize $PageSize -ResultSetSize $PageSize -ErrorAction Stop | Select-Object -ExpandProperty SamAccountName
        $Users += $UserPage
        $Offset += $PageSize
        Write-Output "Received $Offset users..."
    } while ($UserPage.Count -eq $PageSize)
}

if ($Users.Count -eq 0) {
    Write-Output "No users found in OU $SearchBase. Shutting down."
    Stop-Transcript
    exit
}

$TotalUsers = $Users.Count
Write-Output "Total users to process: $TotalUsers"

# Create runspace pool
$RunspacePool = [runspacefactory]::CreateRunspacePool(1, $MaxThreads)
$RunspacePool.Open()
$Jobs = @()

# Scriptblock for user processing
$ScriptBlock = {
    param(
        [string]$SamAccountName,
        [string]$ErrorLogFile
    )
    
    function Process-UserGroups {
        param(
            [string]$SamAccountName,
            [string]$ErrorLogFile
        )
        
        $UserLog = "[$SamAccountName]"
        try {
            $User = Get-ADUser -Identity $SamAccountName -Properties memberof -ErrorAction Stop
            $Groups = Get-ADPrincipalGroupMembership $User | Where-Object { $_.Name -ne "Domain Users" }
            
            if ($Groups.Count -eq 0) {
                Write-Output "$UserLog No groups to delete"
                return
            }
            
            Write-Output "$UserLog Groups found: $($Groups.Count)"
            
            $SuccessCount = 0
            $FailedCount = 0
            $FailedGroups = @()
            
            foreach ($Group in $Groups) {
                try {
                    Remove-ADGroupMember -Identity $Group -Members $User -Confirm:$false -ErrorAction Stop
                    Write-Output "$UserLog Removed from group: $($Group.Name)"
                    $SuccessCount++
                }
                catch {
                    $ErrorMessage = $_.Exception.Message -replace '"', '""'
                    Write-Output "$UserLog ERROR while deleting from $($Group.Name): $ErrorMessage"
                    Add-Content -Path $ErrorLogFile -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss');$SamAccountName;$($Group.Name);$ErrorMessage"
                    $FailedCount++
                    $FailedGroups += $Group.Name
                }
            }
            
            Write-Output "$UserLog Result: successful - $SuccessCount, errors - $FailedCount"
            if ($FailedCount -gt 0) {
                Write-Output "$UserLog Groups with errors: $($FailedGroups -join ', ')"
            }
        }
        catch {
            $ErrorMessage = $_.Exception.Message -replace '"', '""'
            Write-Output "$UserLog ERROR: $_"
            Add-Content -Path $ErrorLogFile -Value "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss');$SamAccountName;GENERAL;$ErrorMessage"
        }
    }
    
    Process-UserGroups -SamAccountName $SamAccountName -ErrorLogFile $ErrorLogFile
}

# Processing users
$Processed = 0
foreach ($User in $Users) {
    $PowerShell = [powershell]::Create()
    $PowerShell.RunspacePool = $RunspacePool
    [void]$PowerShell.AddScript($ScriptBlock)
    [void]$PowerShell.AddParameter('SamAccountName', $User)
    [void]$PowerShell.AddParameter('ErrorLogFile', $ErrorLogFile)
    
    $Job = [PSCustomObject]@{
        PowerShell = $PowerShell
        AsyncResult = $PowerShell.BeginInvoke()
        User = $User
    }
    
    $Jobs += $Job
    $Processed++
    
    Write-Progress -Activity "User processing" -Status "Added to queue: $Processed из $TotalUsers" -PercentComplete ($Processed/$TotalUsers*100)
    
    $CompletedJobs = $Jobs | Where-Object { $_.AsyncResult.IsCompleted -eq $true }
    foreach ($CompletedJob in $CompletedJobs) {
        $CompletedJob.PowerShell.EndInvoke($CompletedJob.AsyncResult)
        $CompletedJob.PowerShell.Dispose()
        $Jobs = $Jobs | Where-Object { $_.User -ne $CompletedJob.User }
    }
}

# Waiting for completion
Write-Output "Waiting for processing to complete..."
while ($Jobs.AsyncResult.IsCompleted -contains $false) {
    $Completed = ($Jobs.AsyncResult.IsCompleted | Where-Object { $_ -eq $true }).Count
    $Remaining = $Jobs.Count - $Completed
    Write-Progress -Activity "Completing processing" -Status "Left: $Remaining" -PercentComplete (($TotalUsers - $Remaining)/$TotalUsers*100)
    Start-Sleep -Seconds 2
}

# Cleaning resources
foreach ($Job in $Jobs) {
    $Job.PowerShell.EndInvoke($Job.AsyncResult)
    $Job.PowerShell.Dispose()
}

$RunspacePool.Close()
$RunspacePool.Dispose()

# Final report
$ErrorCount = (Import-Csv -Path $ErrorLogFile -Delimiter ';').Count - 1
Write-Output "`n=== Processing completed ==="
Write-Output "Total users processed: $TotalUsers"
Write-Output "Errors recorded: $ErrorCount"
Write-Output "Main log: $LogFile"
Write-Output "Error log: $ErrorLogFile"

Stop-Transcript