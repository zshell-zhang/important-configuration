#!usr/bin/expect -f

password="28df49c955"
spawn ssh -p 22 zshell.zhang@123.59.183.83
expect "*passphrase*" 
send "$password\r"
	# "Last login*":{send "yes";exp_continue}
interact
