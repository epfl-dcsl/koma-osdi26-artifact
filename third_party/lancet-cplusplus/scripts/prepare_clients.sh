#!/usr/bin/env bash

# MIT License
#
# Copyright (c) 2019-2021 Ecole Polytechnique Federale Lausanne (EPFL)
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

SSH_OPTS="-o BatchMode=yes -o StrictHostKeyChecking=accept-new"

for hostname in `IFS=',' inarr=($1) && echo ${inarr[@]}`; do
	ssh $SSH_OPTS $hostname "sudo apt-get update && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y python3-pip python3-virtualenv python3-venv libprotobuf-dev libgrpc++-dev libgrpc-dev"
done
