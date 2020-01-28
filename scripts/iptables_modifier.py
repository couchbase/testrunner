from optparse import OptionParser
import paramiko

def main(operation, user, password):
    ssh1 = paramiko.SSHClient()
    ssh1.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh1.connect(hostname="172.23.104.150", username=user, password=password)

    ssh2 = paramiko.SSHClient()
    ssh2.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh2.connect(hostname="172.23.104.136", username=user, password=password)

    if operation == "close":
        print("Closing ports")
        command_1 = "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j REJECT"
        command_2 = "/sbin/iptables -A OUTPUT -p tcp -o eth0 --sport 1000:65535 -j REJECT"

        ssh1.exec_command(command_1)
        ssh1.exec_command(command_2)
        ssh2.exec_command(command_1)
        ssh2.exec_command(command_2)
    elif operation == "open":
        print("Opening ports")
        command = "/sbin/iptables -F"
        ssh1.exec_command(command)
        ssh2.exec_command(command)
    else:
        print("Unknown operation!")
        exit(1)

if __name__ == "__main__":
    usage = ''
    parser = OptionParser(usage)
    parser.add_option('-o','--operation', dest='operation')
    parser.add_option('-u','--user', dest='user')
    parser.add_option('-p','--password', dest='password')

    options, args = parser.parse_args()

    main(options.operation, options.user, options.password)
