h1. What's this?

This is a step-by-step writeup on how to launch nodes on EC2 and run
testrunner against them.

h1. Prerequisties

* Your own AWS login.
* Your own AWS registered keypair
** See AWS -> EC2 -> NETWORK & SECURITY -> Key Pairs
* ssh tools

h1. Step to launch a couchbase server node.

Here are the steps to launch a couchbase 1.8.0 server on EC2.

Click EC2 tab.

Click Getting Started -> "Launch Instance" button.

Use the "Quick Launch Wizard".

Name Your Instance: for example, "SteveClusterX".

Choose a Key Pair: use your key pair, like "steveyen-key".

Choose a Launch Configuration: Amazon Linux AMI, 64-bit
  Recent versions include 2012.03 (ami-e565ba8c) and 2011.09.  Use the latest.

Click Continue button.

Click Edit Details button.

On Instance Details -> Type, choose "m1.large" (or m1.xlarge if you want more power).

On Security Settings -> Select Existing Security Groups, choose "couchbase".

On Advanced Details -> User Data: paste in the following...

    #include https://raw.github.com/couchbaselabs/cloud-init/master/couchbase-server-community_x86_64_1.8.0.rpm.install

More details about using User Data in this way (also called "Cloud Init") are at...

  https://github.com/couchbaselabs/cloud-init

Click Save Details button.

Click Launch button.

h2. Launch more couchbase server nodes.

Repeat the previous steps to launch another (or more) couchbase server node.

h2. Wait until the servers are booted and ready.

Click on the EC2 tab -> Instances.

Search for your nodes by name, like "SteveClusterX".

Once your nodes are in running state and pass the status checks, click on them.  The bottom panel will show details about your node, including their DNS address (aka, HOST).  This will look like...

    ec2-23-22-38-21.compute-1.amazonaws.com

h2. Finish node setup

Browse to http://HOST:8091

For example, in the above case...

    http://ec2-23-22-38-21.compute-1.amazonaws.com:8091

If you have errors, wait a little bit as the software is probably still installing.

You should see the Couchbase login screen.  Use Administrator and password to login.

h2. Join your nodes.

Next, join your nodes together and rebalance them.

h2. To SSH into your new node(s)

Use...

    ssh -i YOUR_KEY.pem ec2-user@HOST

For example...

    ssh -i steveyen-key.pem ec2-user@ec2-23-22-38-21.compute-1.amazonaws.com

After SSH'ing into your node, you can also put load on your configured instance by...

    /opt/couchbase/bin/memcachetest -l

h2. Get testrunner

If not already, in your SSH session, install git...

    sudo yum install git

And, if you like emacs...

    sudo yum install emacs

Then, in your SSH session...

    git clone git://github.com/couchbase/testrunner.git

To test that you got it, try...

    cd testrunner
    ./testrunner
    ./testrunner -h

h2. Get your key to onto the EC2 node

For example...

  scp -i ~/steveyen-key.pem ~/steveyen-key.pem ec2-user@ec2-23-22-38-21.compute-1.amazonaws.com:/home/ec2-user/key.pem

h2. Create a testrunner *.ini file

Create a testrunner *.ini on your EC2 node that lists your EC2 nodes.

For example, the file might be named my-ec2-cluster.ini, with contents like...

    [global]
    username:ec2-user
    ssh_key:/home/ec2-user/key.pem
    
    [membase]
    rest_username:Administrator
    rest_password:password
    
    [servers]
    1:ec2-67-202-9-21.compute-1.amazonaws.com
    2:ec2-23-20-47-95.compute-1.amazonaws.com
    
    [ec2-67-202-9-21.compute-1.amazonaws.com]
    ip:ec2-67-202-9-21.compute-1.amazonaws.com
    port:8091
    
    [ec2-23-20-47-95.compute-1.amazonaws.com]
    ip:ec2-23-20-47-95.compute-1.amazonaws.com
    port:8091

h2. Install paramiko

In your SSH session, use...

    sudo yum install python-paramiko

h2. Run a test

For example, in your SSH session, run...

    ./testrunner -i tmp/fake.ini -t performance.eperf.EPerfMaster.test_ept_scaled_down_write_1 -p mem_quota=4000,items=100000

Notice that the memory quota and number of items is turned down for quick turnaround time.

That's it!

h1. Making and testing changes

Next, you'll probably be wanting to change testrunner, modify tests,
and try them out before pushing them up for code review.

The way this will work is you'll make code/script changes to
testrunner on your own personal dev box/laptop.  Then you'll push out
those changes to your own personal testrunner branch.  Then you'll
pull those changes onto your EC2 node for testing.  That is, editing
doesn't happen on your EC2 node (so, you're treating your EC2 cluster
as your remote compile/run/debug environment).

Here are the steps...

h2. Fork testrunner

Fork couchbase/testrunner to your own personal github account.

Prerequisites: you should have your own github.com account (it's free!)

Log in to http://github.com

Browse to http://github.com/couchbase/testrunner

Hit the "Fork" button

After the hardcore forking action, browse to http://github.com/YOURACCOUNT/testrunner

h2. On your dev box...

In your testrunner project working directory

    git clone git@github.com:couchbase/testrunner.git
    cd testrunner
    git remote add YOURACCOUNT git@github.com:YOURACCOUNT/testrunner.git
    git remote update
    git checkout master
    git branch wip

h2. After you make a change...

After you make changes and "git add" and "git commit", then...

    git push YOURACCOUNT wip

h2. Pull the change to your EC2 node...

In your SSH session, some initialization...

    git remote add YOURACCOUNT git://github.com/YOURACCOUNT/testrunner.git
    git remote update
    git checkout -b wip YOURACCOUNT/wip

Then, to pull down your change...

    git checkout wip
    git remote update
    git pull YOURACCOUNT wip


