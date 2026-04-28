# Makefile - Generic Serial Module


CC = /home/sam9x25/Linux/tools/arm-2011.09/bin/arm-none-linux-gnueabi-gcc

#INCLUDE= -I../
#LIB_DIR= -L/home/sam9x25/CC/redis-5.0.8/deps/hiredis

#CC = /home/iot-gateway/bin/arm-none-linux-gnueabi-gcc

INCLUDE= -I/home/iot-gateway/redis-5.0.8-bin/include
#INCLUDE= -I../
LIB_DIR= -L/home/iot-gateway/redis-5.0.8-bin/lib
#LIB_DIR= -L/home/iot-gateway/hiredis
#LIBS= -lhiredis -levent 
LIBS= -lhiredis 
CFLAGS_TGT=  -DEMBED_TGT=1 

#OBJS= example.o
#OBJS= gprs_vpn_merge.o
OBJS=mqtt_ems_merge.o

TARGET=redis-test

$(TARGET): $(OBJS)
	$(CC) -o $(TARGET) $(OBJS) $(LIB_DIR) $(LIBS) 
	
.c.o:
	$(CC) $(CFLAGS_TGT) -c $^ $(INCLUDE) 

clean::
	rm $(OBJS) $(TARGET)
