

#Gokul mqtt compilation starts ----> 20/02/2026
TARGET = re_mqtt_proc

CC = /home/imx6ul/Toolchain/gcc-linaro-4.9-2014.11-x86_64_arm-linux-gnueabihf/bin/arm-linux-gnueabihf-gcc

INCLUDE = -I../Tools/redis-bin/include -I../Tools/openssl/include -I../Tools/paho/include -Iinclude

CFLAGS = -Wall -O2 -std=gnu99 -I.

LIB_DIR = -L../Tools/redis-bin/lib -L../Tools/openssl/lib -L../Tools/paho/lib -Llib    # ADD THIS IF EXISTS

LIBS = -lpaho-mqtt3as -lhiredis -lssl -lcrypto -lpthread -lm -ldl -ljansson

# SRC = $(wildcard *.c)
# OBJ = $(SRC:.c=.o)
SRCDIR   = src
SRC = $(wildcard $(SRCDIR)/*.c)
OBJ = $(SRC:.c=.o)

all: re_mqtt_proc
	@echo ""
	@echo "================================== $(TARGET) COMPILATION SUCCESS ==============================="
	@echo ""
	@echo ""

TARGET = re_mqtt_proc

$(TARGET): $(OBJ)
	@echo ""
	@echo "================================== $(TARGET) COMPILATION STARTS ==============================="
	@echo ""
	@echo ""
	$(CC) -o $@ $^ $(LIB_DIR) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) $(INCLUDE) -c $< -o $@

clean:
	rm -f *.o $(TARGET)


