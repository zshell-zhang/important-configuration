#!/bin/bash
# qunar ssh remote server alias

function sb() {
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

function st() {
  if [[ -n "$1" ]] ;then
      echo "zshell.zhang@l-rtools1.ops.cn$1.qunar.com"
      ssh zshell.zhang@l-rtools1.ops.cn$1.qunar.com
  fi
}

