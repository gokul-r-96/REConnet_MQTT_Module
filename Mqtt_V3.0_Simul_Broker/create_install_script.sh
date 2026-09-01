#!/bin/sh
VERSION_CODE="$(echo "Hello Hai Hi nana..." | awk '{print $2}')"
#VERSION_CODE={ls} 
#VERSION_CODE="$(echo | awk -v home=$HOME '{print "My home is " home}')"
echo $VERSION_CODE


#exit 0
echo $1
echo $2

if [ "$1" = "" ]
then
	echo "Enter the version : PROD/UPGRADE"
	exit
elif [ "$1" = "PROD" ]
then
	echo "Production version"
elif [ "$1" = "UPGRADE" ]
then
	echo "Upgrade version"
else
	echo "Enter the version : PROD/UPGRADE"
	exit
fi

HARDWARE=$2
VER="$1"
echo $VER
echo ""
echo "DCU"

if [ "$HARDWARE" = "IMX" ]
        then
                echo "copy imxcc to make.inc"
                cp make.imxcc make.inc
#             cp -Rf imx6ul/home dcuFirmware/
#            cp -Rf imx6ul/etc dcuFirmware/
        else
                echo "copy samcc to make.inc"
                cp make.samcc make.inc
#                cp -Rf sam9x25/home dcuFirmware/
#                cp -Rf sam9g/home/* dcuFirmware/home/
#                cp -Rf sam9g/etc dcuFirmware/
fi

echo "Creating DCUFeatures json file"
cd ./genDcuJsonFile
make clean;make
./genDcuJsonFile

eth=0
tcs_flag=0
filename="dcuFeatures.json"
substring1=Ethernet
substring2=2
substring3=4
substring4=1
substring5=SerType
substring6=5

while read -r line
do
    name="$line"
        echo "Line read from file - $name"
        if printf %s\\n "${name}" | grep -qF "${substring1}"; then
                echo "Line contains ethernet"
                if printf %s\\n "${name}" | grep -qF "${substring2}"; then
                        echo "Line contains Ethernet:2"
                        eth=2
                        break
                elif printf %s\\n "${name}" | grep -qF "${substring3}"; then
                        echo "Line contains Ethernet:4"
                        eth=4
                        break
                elif printf %s\\n "${name}" | grep -qF "${substring4}"; then
                        echo "Line contains Ethernet:1"
                        eth=1
                        break
                fi

        fi
        if printf %s\\n "${name}" | grep -qF "${substring5}"; then
                echo "Line contains SerType"
                if printf %s\\n "${name}" | grep -qF "${substring6}"; then
                        echo "Line contains SerType:5"
                        tcs_flag=1
                        #break
		fi
	fi

done < "$filename"

cd ..

rm -Rf dcuFirmware
cp -Rf dcuFirmwareAll dcuFirmware
rm -Rf dcuFirmware/lib
rm -Rf dcuFirmware/lib_sam
rm -Rf lib

if [ "$HARDWARE" = "IMX" ]
then
	echo "copy imx related lib files"
	cp -Rf dcuFirmwareAll/lib lib
	cp -Rf imx6ul/home dcuFirmware/
	cp -Rf imx6ul/etc dcuFirmware/
else
	echo "copy sam related lib files"
	cp -Rf dcuFirmwareAll/lib_sam lib
	cp -Rf sam9x25/home dcuFirmware/
	cp -Rf sam9g/home/* dcuFirmware/home/
	cp -Rf sam9g/etc dcuFirmware/

fi

echo " tcs_flag -- $tcs_flag"

if [ $tcs_flag = 0 ]
then
	echo "Copying old htdoc files..."
	cp -Rf dcuFirmware/srv/www/htdocs_old/htdocs dcuFirmware/srv/www/
else
#	echo "TCS version, copying new auth file"
#	cp auth.json.new dcuFirmware/srv/www/htdocs/auth.json
#	cp auth.json.new dcuFirmware/usr/cms/def/auth.json
	echo "TCS version, copying new nw check files"
	cp create_nw_gw_check.sh dcuFirmware/etc/
	cp run_nw_check.sh dcuFirmware/etc/
	cp rc.local.nw_check dcuFirmware/etc/rc.local
fi

echo "Copying defautl files"
cd ./genDcuJsonFile
#make clean;make
#./genDcuJsonFile
cp config.cfg ../dcuFirmware/usr/cms/config/
cp config_prev.cfg ../dcuFirmware/usr/cms/config/
cp config.cfg ../dcuFirmware/usr/cms/def/
cp config.cfg ../dcuFirmware/srv/www/htdocs/
cp ../dcuFirmware/srv/www/htdocs/auth.json ../dcuFirmware/usr/cms/def/
cp version.txt ../dcuFirmware/srv/www/htdocs/info/
cp dcuConfig.json ../dcuFirmware/srv/www/htdocs/dcuConfig.json
cp dcuConfig.json ../dcuFirmware/srv/www/htdocs/temp_dcuConfig.json
cp dcuFeatures.json ../dcuFirmware/srv/www/htdocs/dcuFeatures.json


cd ..
#Preethi 11Apr2k25 
#copy new auth files created
cp auth.json dcuFirmware/srv/www/htdocs/auth.json
cp auth.json dcuFirmware/usr/cms/def/auth.json


echo -n "Firmware Version:  "
VERSION=`cat include/version.h | awk -F\" '{print $2}'`
echo $VERSION
echo ""

echo "building monProc ..."
cd ./Pmon
make clean;make
cd ..
cp ./Pmon/dcuMonProc bin/.
echo ""

echo "building modRtuProc ..."
cd ./modMeterDcuProc
make clean;make
cd ..
cp ./modMeterDcuProc/dcuModMetProc bin/.
echo ""

echo "building 104sProc ..."
cd ./Iec104sProc
make clean;make
cd ..
cp ./Iec104sProc/dcuIec104S bin/.
echo ""

echo "building FtpProc ..."
cd ./dcu_ftp_proc
make clean;make
cd ..
cp ./dcu_ftp_proc/dcu_ftp_proc bin/.
echo ""

echo "building dlmsProc ..."
cd ./dcuDlmsProc
make clean;make
cd ..
cp ./dcuDlmsProc/dcuDlmsMetProc bin/.
echo ""

echo "building dlmsEthProc ..."
cd ./dcuDlmsEthProc
make clean;make
cd ..
cp ./dcuDlmsEthProc/dcuDlmsEthMetProc bin/.
echo ""

echo "building StatusProc ..."
cd ./Status
make clean;make
cd ..
cp ./Status/dcuStatusJson bin/.
echo ""

echo "building connMonProc ..."
cd ./connMonProc
make clean;make
cd ..
cp ./connMonProc/connMonProc bin/.
echo ""

echo "building sigStrenProc ..."
cd ./sigStren
make clean;make
cd ..
cp ./sigStren/sigStrenProc bin/.
echo ""

echo "building pingProc ..."
cd ./pingProc
make clean;make
cd ..
cp ./pingProc/pingProc bin/.
echo ""

echo "building ipSecMonProc ..."
cd ./ipSecMonProc
make clean;make
cd ..
cp ./ipSecMonProc/ipSecMonProc bin/.
echo ""

echo "building RMSProc ..."
cd ./RMS
make clean;make
cd ..
cp ./RMS/dcuRMSProc bin/.
echo ""

echo "building RMS_MQTTProc ..."
cd ./RMS_MQTT
make clean;make
cd ..
cp ./RMS_MQTT/dcuRMSMQTTProc bin/.
echo ""

echo "building cmsRMSProc ..."
cd ./cmsRms
make clean;make
cd ..
cp ./cmsRms/cmsRMSProc bin/.
echo ""

echo "building ntpClientProc ..."
cd ./ntpclient
make clean;make
cd ..
cp ./ntpclient/dcuNtpClientProc bin/.
echo ""
echo ""
echo "building modTcpSlaveProc ..."
cd ./ModTcpSlave
make clean;make
cd ..
cp ./ModTcpSlave/dcuModTcpSProc bin/.
echo ""
echo ""
echo "building createNetworkInfProc ..."
cd ./createNetworkInf
make clean;make
cd ..
cp ./createNetworkInf/createNetworkInf bin/.
echo ""
echo "building resetSwitchProc ..."
cd ./resetSwitchProc
make clean;make
cd ..
cp ./resetSwitchProc/resetSwitchProc bin/.
echo ""
#preethi 03Apr2k25
echo "building encrypt pwd proc ..."
cd ./encrypt_pwd
make clean;make
cd ..
cp ./encrypt_pwd/encrypt_pwd bin/.
echo ""


cp bin/* dcuFirmware/usr/cms/bin/
echo ""
echo ""
echo "building WebRel"
cd ./WebRel
make clean;make
cd ..
cp WebRel/cgi-bin/* dcuFirmware/srv/www/htdocs/cgi-bin/


echo "Creating FIRMWARE ..."
if [ $VER = "PROD" ]
then
	echo "Production version"
	if [ $eth = 2 ]
	then
		echo "Two ethernet ports"
		cp -Rf network dcuFirmware/etc/
		
		echo $HARDWARE

		if [ "$HARDWARE" = "IMX" ]
		then
			mv dcuFirmware/etc/network/interfaces4 dcuFirmware/etc/network/interfaces
		else
			mv dcuFirmware/etc/network/interfaces2 dcuFirmware/etc/network/interfaces
		fi

	elif [ $eth = 4 ]
	then
		echo "Four ethernet ports"
		cp -Rf network dcuFirmware/etc/
		mv dcuFirmware/etc/network/interfaces3 dcuFirmware/etc/network/interfaces
		rm dcuFirmware/etc/network/interfaces2
	else
		echo "One ethernet port"
		cp -Rf network dcuFirmware/etc/
		rm dcuFirmware/etc/network/interfaces2
		rm dcuFirmware/etc/network/interfaces3
	fi	
	
	if [ "$HARDWARE" = "IMX" ]
	then
		cp newInstall.sh.IMX ../../DcuReleaseDir/newInstall.sh
	else
		cp newInstall.sh ../../DcuReleaseDir/newInstall.sh
	fi
else
	echo "Upgrade version"	
	rm -Rf dcuFirmware/home
	rm -Rf dcuFirmware/etc/network
	echo "No VPN upgrade version..."
#	cp newInstall.sh ../DcuReleaseDir/newInstall.sh
	if [ "$HARDWARE" = "IMX" ]
	then
		cp  newInstall.sh.IMX_upgrade  ../../DcuReleaseDir/newInstall.sh
	else
		cp newInstall.sh.Upgrade ../../DcuReleaseDir/newInstall.sh
	fi
fi

echo "tar dcuFirmware"
#tar -cvf $VERSION.tar sys_bin/ abb_bin/ abb_cfg/  sys_etc/ sys_usr_sbin 
tar -cvf dcuFirmware.tar dcuFirmware 
mv dcuFirmware.tar ../../DcuReleaseDir/
#preethi
tar -cvf lib.tar lib 
mv lib.tar ../../DcuReleaseDir/

if [ "$HARDWARE" = "IMX" ]
then
	echo "In IMX--> tar strongswan"
	cd imx6ul
	tar -cvf strongswan-5.8.2.tar strongswan-5.8.2
	cd ..

	mv imx6ul/strongswan-5.8.2.tar ../../DcuReleaseDir/

else
	echo "IN SAM strongswan not available"
	cd sam9x25

	tar -cvf strongswan-5.8.2.tar strongswan-5.8.2
	cd ..
	mv sam9x25/strongswan-5.8.2.tar ../../DcuReleaseDir/

fi


echo "building genFwMd5checksum"
cd ./dcu_gen_md5_chk_sum_proc
make clean;make
./dcu_gen_md5_proc 0

cd ..

# if [ $vpn == 1 ]; then
	# ./dcu_gen_md5_proc 1
# else
	# ./dcu_gen_md5_proc 0
# fi	
# cd ..

VERSION_CODE="$(echo $VERSION | awk '{print $1}')"
echo $VERSION_CODE
VERSION_CODE_DATE="$(echo $VERSION | awk '{print $2 "_" $3}')"
echo $VERSION_CODE_DATE

WEBFWDIR=$VERSION_CODE"_"$VERSION_CODE_DATE
echo $WEBFWDIR

mkdir -p ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER  
cp ../../DcuReleaseDir/newInstall.sh ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER/
cp ../../DcuReleaseDir/dcuFirmware.tar ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER/
cp ../../DcuReleaseDir/lib.tar ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER/
if [ "$HARDWARE" = "IMX" ]
then
	echo "IN IMX copy strongswan to release dir"
	cp ../../DcuReleaseDir/strongswan-5.8.2.tar ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER/
else
	echo "IN SAM strongswan to release dir not available"
	cp ../../DcuReleaseDir/strongswan-5.8.2.tar ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER/
fi

cp ReleaseNotes.txt ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER/
cp dcu_gen_md5_chk_sum_proc/checksum ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/$VER/
cp dcu_gen_md5_chk_sum_proc/checksum ../../DcuReleaseDir/checksum

echo "Creating firmware for web upload"
rm -Rf ../../DcuReleaseDir/$WEBFWDIR  
mkdir -p ../../DcuReleaseDir/$WEBFWDIR 

cp ../../DcuReleaseDir/newInstall.sh ../../DcuReleaseDir/$WEBFWDIR
cp ../../DcuReleaseDir/dcuFirmware.tar ../../DcuReleaseDir/$WEBFWDIR
cp ../../DcuReleaseDir/lib.tar ../../DcuReleaseDir/$WEBFWDIR
if [ "$HARDWARE" = "IMX" ]
then
	echo "IN IMX copy strongswan to dir web_fw "
	cp ../../DcuReleaseDir/strongswan-5.8.2.tar ../../DcuReleaseDir/$WEBFWDIR
else
	echo "IN SAM copy strongswan to dir web_fw "
	cp ../../DcuReleaseDir/strongswan-5.8.2.tar ../../DcuReleaseDir/$WEBFWDIR
fi
cp ../../DcuReleaseDir/checksum ../../DcuReleaseDir/$WEBFWDIR

echo "Copying Web FW bin"
cd ../../DcuReleaseDir
rm -Rf $WEBFWDIR.zip  
zip -r $WEBFWDIR.zip $WEBFWDIR
cp $WEBFWDIR.zip $VERSION_CODE/$VERSION_CODE_DATE/$VER/$WEBFWDIR.bin
cd -

echo "Firmware Created Successfully !!"

echo ""
echo -n "Firmware Version:  "
echo $VERSION


# rm -Rf ../../gitDCURelRepo/Release/* 
# mkdir -p ../../gitDCURelRepo/Release 
# mkdir -p ../../gitDCURelRepo/Source 
# mkdir -p ../../gitDCURelRepo/Release/$VERSION_CODE 
# rm -Rf ../../gitDCURelRepo/Source/* 
# mkdir -p ../../gitDCURelRepo/Source/$VERSION_CODE 
# mkdir -p ../../gitDCURelRepo/Source/$VERSION_CODE/$VERSION_CODE_DATE 
# cp -Rf ../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/ ../../gitDCURelRepo/Release/$VERSION_CODE/
# zip -r ../DcuSource.zip ../DcuSource
# mv ../DcuSource.zip ../../gitDCURelRepo/Source/$VERSION_CODE/$VERSION_CODE_DATE/
# echo "Copied release for backup..."

#mkdir -p ../../gitDCURelRepo/Release/$VERSION_CODE
#mkdir -p ../../gitDCURelRepo/Source/$VERSION_CODE
#mkdir -p ../../gitDCURelRepo/Source/$VERSION_CODE/$VERSION_CODE_DATE
#cp -Rf RTU_Install_Dir/$VERSION_CODE/$VERSION_CODE_DATE/ ../../gitDCURelRepo/Release/$VERSION_CODE/
#mv RTU_Latest_All.zip ../../gitDCURelRepo/Source/$VERSION_CODE/$VERSION_CODE_DATE/
# cp create_install_script.sh ../../gitDCURelRepo/
# cp create_rtu_versions.sh ../../gitDCURelRepo/

echo "Copying release for backup..."
sep_var="_"
ver_name="$VERSION_CODE$sep_var$VERSION_CODE_DATE"
pwd
#cd ..
rm -Rf ../Release/* 
mkdir -p ../Release 
mkdir -p ../Release/$ver_name
cp -Rf ../../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/* ../Release/$ver_name/
if [ $VER = "UPGRADE" ]
then
	echo "**************** Creating smaller upgrade version *************"
	WEBFWDIR2=$VERSION_CODE"_"$VERSION_CODE_DATE"_2"
	mkdir -p ../Release/$ver_name/$VER/$WEBFWDIR2
	cp ../Release/$ver_name/$VER/newInstall.sh ../Release/$ver_name/$VER/$WEBFWDIR2/
	cp ../Release/$ver_name/$VER/dcuFirmware.tar ../Release/$ver_name/$VER/$WEBFWDIR2/
	cp ../Release/$ver_name/$VER/lib.tar ../Release/$ver_name/$VER/$WEBFWDIR2/
	cp ../Release/$ver_name/$VER/checksum ../Release/$ver_name/$VER/$WEBFWDIR2/
	rm -Rf ../Release/$ver_name/$VER/$WEBFWDIR2.zip  
	cd ../Release/$ver_name/$VER/
	zip -r $WEBFWDIR2.zip $WEBFWDIR2
	mv $WEBFWDIR2.zip $WEBFWDIR2.bin
	rm -Rf $WEBFWDIR2
	echo "***** Created smaller upgrade version ****************"
	cd -
	pwd
else
	echo "******* Production version *********"
fi	

#copy 

echo $ver_name >> ../Release/version.txt
echo $ver_name >> version.txt
echo $ver_name
echo "Copied release for backup..."

sep_var="_"
ver_name="$VERSION_CODE$sep_var$VERSION_CODE_DATE"

rm -Rf ../../gitDCURelRepo/Release/*
mkdir -p ../../gitDCURelRepo/Release
mkdir -p ../../gitDCURelRepo/Source
rm -Rf ../../gitDCURelRepo/Source/*
cp -Rf ../DcuReleaseDir/$VERSION_CODE/$VERSION_CODE_DATE/* ../../gitDCURelRepo/Release/
echo $ver_name >> ../../gitDCURelRepo/Release/version.txt
zip -r ../DcuSource.zip ../DcuSource
mv ../DcuSource.zip ../../gitDCURelRepo/Source/
echo $ver_name >> ../../gitDCURelRepo/Source/version.txt
echo "Copied release for backup..."


