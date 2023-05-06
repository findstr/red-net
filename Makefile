SRC := adlist.c\
	adlist.c\
	ae.c\
	anet.c\
	dict.c\
	main.c\
	timeout.c\
	util.c\
	syscheck.c\
	monotonic.c\
	rax.c\
	redisassert.c\
	sds.c\
	sha256.c\
	sha1.c\
	zmalloc.c\

all:
	gcc -o red-net $(SRC) -lm
