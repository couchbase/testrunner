# This script used to run with ssh.py under scripts directory
# How to run:
# python3 scripts/ssh.py  -i your_bad_vms.ini -p script=scripts/windows_cleanup_registry.sh
#
taskkill /F /T /IM msiexec.exe
taskkill /F /T /IM erl.exe
taskkill /F /T /IM epmd.exe

echo -e "delete HKEY_CLASSES_ROOT\Installer\UpgradeCodes\DAFE44492BF730D45B002C1133EA9A42 \n"
reg delete "HKEY_CLASSES_ROOT\Installer\UpgradeCodes\DAFE44492BF730D45B002C1133EA9A42" /f
echo -e "delete HKEY_CLASSES_ROOT\Installer\Products\288F9D42C18440B25027002B37EC713B \n"
reg delete "HKEY_CLASSES_ROOT\Installer\Products\288F9D42C18440B25027002B37EC713B" /f
# for sherloc, delete key below
reg delete "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall\InstallShield_{24D9F882-481C-2B04-0572-00B273CE17B3}" /f  # 4.0.0
reg delete "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall\InstallShield_{898C4818-1F6D-C554-1163-6DF5C0F1F7D8}" /f  # 4.1.0
reg delete "HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\Microsoft\Windows\CurrentVersion\Uninstall\InstallShield_{A4BB2687-E63E-F424-F9F3-18D739053798}" /f  # 4.5.0
reg delete "HKEY_CLASSES_ROOT\Installer\Products\5E5D7293-AC1D-3424-E583-0644411FDA20" /f	# 3.1.0
reg delete "HKEY_CLASSES_ROOT\Installer\Products\24D9F882-481C-2B04-0572-00B273CE17B3" /f        # 4.0
reg delete "HKEY_CLASSES_ROOT\Installer\Products\41276A8D-2A65-88D4-BDCC-8C4FE109F4B8" /f         # 3.1.1
reg delete "HKEY_CLASSES_ROOT\Installer\Products\F0794D16-BD9D-4638-9EEA-0E591F170BD7" /f         # 3.1.2
reg delete "HKEY_CLASSES_ROOT\Installer\Products\898C4818-1F6D-C554-1163-6DF5C0F1F7D8" /f         # 4.1.0
reg delete "HKEY_CLASSES_ROOT\Installer\Products\8184C898D6F1455C1136D65F0C1F7F8D" /f             # 4.1.0
reg delete "HKEY_CLASSES_ROOT\Installer\Products\A4BB2687-E63E-F424-F9F3-18D739053798" /f         # 4.5.0
rm -rf /cygdrive/c/Program\ Files/Couchbase/Server
