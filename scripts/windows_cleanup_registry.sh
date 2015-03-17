# This script used to run with ssh.py under scripts directory
# How to run:
# python scripts/ssh.py  -i your_bad_vms.ini -p script=scripts/windows_cleanup_registry.sh
#
echo -e "delete HKEY_CLASSES_ROOT\Installer\UpgradeCodes\DAFE44492BF730D45B002C1133EA9A42 \n"
reg delete "HKEY_CLASSES_ROOT\Installer\UpgradeCodes\DAFE44492BF730D45B002C1133EA9A42" /f
echo -e "delete HKEY_CLASSES_ROOT\Installer\Products\288F9D42C18440B25027002B37EC713B \n"
reg delete "HKEY_CLASSES_ROOT\Installer\Products\288F9D42C18440B25027002B37EC713B" /f
# for sherloc, delete key below
reg delete "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall\InstallShield_{24D9F882-481C-2B04-0572-00B273CE17B3}" /f
##
echo -e "Start delete folder c/Program\ Files/Couchbase/Serve \n"
rm -rf /cygdrive/c/Program\ Files/Couchbase/Server
