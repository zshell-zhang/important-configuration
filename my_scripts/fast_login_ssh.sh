#!/bin/bash
# qunar ssh remote server alias

function ssh_zshell() {
  if [[ -z "$1" ]]; then
	echo "sb 1"
	echo "e10445d33c"  #zhi.zhang
        ssh -p 22 zhi.zhang@10.64.0.11
  elif [[ "$1"x = "1"x ]]; then
	echo "1"
	echo "e10445d33c"  #zhi.zhang
        ssh -p 22 zhi.zhang@10.64.0.11
  elif [[ "$1"x = "2"x ]]; then
	echo "2"
	echo "e10445d33c"  #zhi.zhang
        ssh -p 22 zhi.zhang@10.64.0.12
  else
	echo "sb 11"
	echo "e10445d33c"  #zhi.zhang
        ssh -p 22 zhi.zhang@10.64.0.11
  fi
}

function sshb() {
  if [[ -z "$1" ]]; then
        echo "hongyu 1"
        echo "e813927176"
        ssh -p 22 hongyu.shao@10.64.0.11
  elif [[ "$1"x = "1"x ]]; then
        echo "hongyu 1"
        echo "e813927176"
        ssh -p 22 hongyu.shao@10.64.0.11
  elif [[ "$1"x = "2"x ]]; then
        echo "hongyu 2"
        echo "e813927176"
        ssh -p 22 hongyu.shao@10.64.0.12
  else
        echo "hongyu 1"
        echo "e813927176"
        ssh -p 22 hongyu.shao@10.64.0.11
  fi
}

function sshq() {
  if [[ -n "$1" ]] ;then
      echo "zshell.zhang@l-rtools1.ops.cn$1.qunar.com"
      ssh zshell.zhang@l-rtools1.ops.cn$1.qunar.com
  else
      ssh zshell.zhang@l-rtools1.ops.cn2.qunar.com
  fi
}

function ssh_cloud() {
  ssh -p 22 -i ~/.ssh/id_rsa root@167.99.57.155
}
