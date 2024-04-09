import pickle, logging 
import xmlrpc.client, socket, time

# For locks: RSM_UNLOCKED=0 , RSM_LOCKED=1 
RSM_UNLOCKED = bytearray(b'\x00') * 1
RSM_LOCKED = bytearray(b'\x01') * 1
RSM_BLOCK = 0

# server address - default is 127.0.0.1, localhost
SERVER_ADDRESS = '127.0.0.1'
MAX_CLIENTS = 8
SOCKET_TIMEOUT = 5
RETRY_INTERVAL = 10

##### File system constants

# Core parameters
# Total number of blocks in raw strorage
TOTAL_NUM_BLOCKS = 256
# Block size (in Bytes)
BLOCK_SIZE = 128
# Maximum number of inodes
MAX_NUM_INODES = 16
# Size of an inode (in Bytes)
INODE_SIZE = 16
# Maximum file name (in characters)
MAX_FILENAME = 12
# Number of Bytes to store an inode number in directory entry
INODE_NUMBER_DIRENTRY_SIZE = 4

# Derived parameters
# Number of inodes that fit in a block
INODES_PER_BLOCK = BLOCK_SIZE // INODE_SIZE

# To be consistent with book, block 0 is root block, 1 superblock
# Bitmap of free blocks starts at offset 2
FREEBITMAP_BLOCK_OFFSET = 2

# Number of blocks needed for free bitmap
# For simplicity, we assume each entry in the bitmap is a Byte in length
# This allows us to avoid bit-wise operations
FREEBITMAP_NUM_BLOCKS = TOTAL_NUM_BLOCKS // BLOCK_SIZE

# inode table starts at offset 2 + FREEBITMAP_NUM_BLOCKS
INODE_BLOCK_OFFSET = 2 + FREEBITMAP_NUM_BLOCKS

# inode table size
INODE_NUM_BLOCKS = (MAX_NUM_INODES * INODE_SIZE) // BLOCK_SIZE

# maximum number of blocks indexed by inode
# This implementation hardcodes:
#   4 bytes for size
#   2 bytes for type
#   2 bytes for refcnt
#   4 bytes per block number index
# In total, 4+2+2+4=8 bytes are used for size+type+refcnt+gencnt, remaining bytes for block numbers
INODE_METADATA_SIZE = 12
MAX_INODE_BLOCK_NUMBERS = (INODE_SIZE - INODE_METADATA_SIZE) // 4

# maximum size of a file
# maximum number of entries in an inode's block_numbers[], times block size
MAX_FILE_SIZE = MAX_INODE_BLOCK_NUMBERS*BLOCK_SIZE

# Data blocks start at INODE_BLOCK_OFFSET + INODE_NUM_BLOCKS
DATA_BLOCKS_OFFSET = INODE_BLOCK_OFFSET + INODE_NUM_BLOCKS

# Number of data blocks
DATA_NUM_BLOCKS = TOTAL_NUM_BLOCKS - DATA_BLOCKS_OFFSET

# Size of a directory entry: file name plus inode size
FILE_NAME_DIRENTRY_SIZE = MAX_FILENAME + INODE_NUMBER_DIRENTRY_SIZE

# Number of filename+inode entries that can be stored in a single block
FILE_ENTRIES_PER_DATA_BLOCK = BLOCK_SIZE // FILE_NAME_DIRENTRY_SIZE


# Supported inode types
INODE_TYPE_INVALID = 0
INODE_TYPE_FILE = 1
INODE_TYPE_DIR = 2
INODE_TYPE_SYM = 3

#### BLOCK LAYER 

# Now the disk block data is remote, in the server - DiskBlocks() is a client
# args contains the command-line arguments passed from the memoryfs_shell.py invocation:
# cid: client ID
# port: server port

class DiskBlocks():
  def __init__(self, args):

    # initialize clientID 
    if args.cid>=0 and args.cid<MAX_CLIENTS:
      self.clientID = args.cid
    else:
      print('Must specify valid cid')
      quit()

    # initialize XMLRPC client connection to raw block server
    if args.startport:
      self.STARTPORT = args.startport
    else:
      print('Must specify startport number')
      quit()

    if args.ns:
      self.NS = args.ns
    else:
      print('Must specify number of servers')
      quit()

    self.block_server = {}

    for PORT in range(self.STARTPORT,self.STARTPORT+self.NS):
      server_url = 'http://' + SERVER_ADDRESS + ':' + str(PORT)
      self.block_server[PORT-self.STARTPORT] = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)

    socket.setdefaulttimeout(SOCKET_TIMEOUT)

    self.HandleFSConstants(args)


# Old
#class DiskBlocks():
#  def __init__(self, args):
#
#    self.HandleFSConstants(args)
#    # This class stores the raw block array
#    self.block = []                                            
#    # Initialize raw blocks 
#    for i in range (0, TOTAL_NUM_BLOCKS):
#      putdata = bytearray(BLOCK_SIZE)
#      self.block.insert(i,putdata)

  #HandleFSConstants: Modify the FS constants if provided in command line argument

  def HandleFSConstants(self, args):

    if args.total_num_blocks:
      global TOTAL_NUM_BLOCKS
      TOTAL_NUM_BLOCKS = args.total_num_blocks

    if args.block_size:
      global BLOCK_SIZE
      BLOCK_SIZE = args.block_size

    if args.max_num_inodes:
      global MAX_NUM_INODES
      MAX_NUM_INODES = args.max_num_inodes

    if args.inode_size:
      global INODE_SIZE
      INODE_SIZE = args.inode_size


    # Modify Derived parameters
    global INODES_PER_BLOCK, FREEBITMAP_BLOCK_OFFSET, FREEBITMAP_NUM_BLOCKS, \
      INODE_BLOCK_OFFSET, MAX_INODE_BLOCK_NUMBERS, MAX_FILE_SIZE, DATA_BLOCKS_OFFSET, \
      DATA_NUM_BLOCKS, FILE_NAME_DIRENTRY_SIZE, FILE_ENTRIES_PER_DATA_BLOCK, INODE_NUM_BLOCKS

    # Number of inodes that fit in a block
    INODES_PER_BLOCK = BLOCK_SIZE // INODE_SIZE

    FREEBITMAP_NUM_BLOCKS = TOTAL_NUM_BLOCKS // BLOCK_SIZE

    # inode table starts at offset 2 + FREEBITMAP_NUM_BLOCKS
    INODE_BLOCK_OFFSET = 2 + FREEBITMAP_NUM_BLOCKS

    # inode table size
    INODE_NUM_BLOCKS = (MAX_NUM_INODES * INODE_SIZE) // BLOCK_SIZE

    MAX_INODE_BLOCK_NUMBERS = (INODE_SIZE - INODE_METADATA_SIZE) // 4


    MAX_FILE_SIZE = MAX_INODE_BLOCK_NUMBERS * BLOCK_SIZE

    # Data blocks start at INODE_BLOCK_OFFSET + INODE_NUM_BLOCKS
    DATA_BLOCKS_OFFSET = INODE_BLOCK_OFFSET + INODE_NUM_BLOCKS

    # Number of data blocks
    DATA_NUM_BLOCKS = TOTAL_NUM_BLOCKS - DATA_BLOCKS_OFFSET

    # Size of a directory entry: file name plus inode size
    FILE_NAME_DIRENTRY_SIZE = MAX_FILENAME + INODE_NUMBER_DIRENTRY_SIZE

    # Number of filename+inode entries that can be stored in a single block
    FILE_ENTRIES_PER_DATA_BLOCK = BLOCK_SIZE // FILE_NAME_DIRENTRY_SIZE

  def getParityServer(self, virtual_block_number):
    #RAID 5
    row = virtual_block_number // (self.NS - 1)
    server_id = (self.NS - 1) - (row % self.NS)
    return server_id

  def getBlockInfo(self, virtual_block_number):
    #RAID 5
    server_id = virtual_block_number % (self.NS - 1)
    block_number = virtual_block_number // (self.NS - 1)
    parity_server_id = (self.NS - 1) - (block_number % self.NS)
    if parity_server_id <= server_id:
      server_id = (server_id + 1) % self.NS
    return server_id, block_number

  ## Put: interface to write a raw block of data to the block indexed by block number
## Blocks are padded with zeroes up to BLOCK_SIZE
  def Put(self, virtual_block_number, block_data):
    parity_server_id = self.getParityServer(virtual_block_number)
    server_id, block_number = self.getBlockInfo(virtual_block_number)
    try:
      if server_id not in self.block_server:
        raise ConnectionRefusedError
      data_read = self.SingleGet(server_id,block_number)
      if data_read == -1:
        print("CORRUPTED_BLOCK " + str(virtual_block_number))
        new_parity = bytearray(block_data.ljust(BLOCK_SIZE,b'\x00'))
        for other_server_id in range(0, self.NS):
          if other_server_id != server_id and other_server_id != parity_server_id:
            A = self.SingleGet(other_server_id, block_number)
            for i in range(BLOCK_SIZE):
              new_parity[i] = new_parity[i] ^ A[i]
        return self.SinglePut(parity_server_id, block_number, new_parity)
    except ConnectionRefusedError:
      print("SERVER_DISCONNECTED PUT " + str(server_id))
      if server_id in self.block_server:
        self.block_server.pop(server_id)
      new_parity = bytearray(block_data.ljust(BLOCK_SIZE, b'\x00'))
      for other_server_id in range(0, self.NS):
        if other_server_id != server_id and other_server_id != parity_server_id:
          A = self.SingleGet(other_server_id, block_number)
          for i in range(BLOCK_SIZE):
            new_parity[i] = new_parity[i] ^ A[i]
      return self.SinglePut(parity_server_id, block_number, new_parity)

    new_parity = bytearray(block_data.ljust(BLOCK_SIZE,b'\x00'))
    self.SinglePut(server_id, block_number, block_data)

    try:
      if parity_server_id not in self.block_server:
        raise ConnectionRefusedError
      parity_read = self.SingleGet(parity_server_id,block_number)
      if parity_read == -1:
        #do nothing (no point writing parity if server has corrupt flag) - would write data if corrupt was not simulated
        return 0
      for i in range(BLOCK_SIZE):
        new_parity[i] = new_parity[i] ^ data_read[i]
      for i in range(BLOCK_SIZE):
        new_parity[i] = new_parity[i] ^ parity_read[i]
      self.SinglePut(parity_server_id, block_number, new_parity)
    except ConnectionRefusedError:
      if parity_server_id in self.block_server:
        self.block_server.pop(parity_server_id)
      print("SERVER_DISCONNECTED PUT " + str(parity_server_id))
      return 0
      # do nothing more (parity can not be written if server is down)
    return 0

  def SinglePut(self, server_id, block_number, block_data):

    logging.debug ('Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
    if len(block_data) > BLOCK_SIZE:
      logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
      quit()

    if block_number in range(0,TOTAL_NUM_BLOCKS): 
      # ljust does the padding with zeros
      putdata = bytearray(block_data.ljust(BLOCK_SIZE,b'\x00'))
      # Write block
      # commenting this out as the request now goes to the server
      # self.block[block_number] = putdata
      # call Put() method on the server; code currently quits on any server failure 
      ret = self.block_server[server_id].Put(block_number,putdata)
      if ret == -1:
        logging.error('Put: Server returns error')
        quit()
      return 0
    else:
      logging.error('Put: Block out of range: ' + str(block_number))
      quit()


## Get: interface to read a raw block of data from block indexed by block number
## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

  def Get(self, virtual_block_number):
    server_id, block_number = self.getBlockInfo(virtual_block_number)
    try:
      if server_id not in self.block_server:
        raise ConnectionRefusedError
      result = self.SingleGet(server_id, block_number)
      if result == -1:
        print("CORRUPTED_BLOCK " + str(virtual_block_number))
        result = self.SingleGet((server_id + 1) % self.NS, block_number)
        if result == -1:
          quit()
        for other_server_id in range(0,self.NS):
          if other_server_id != server_id and other_server_id != ((server_id + 1) % self.NS):
            A = self.SingleGet(other_server_id,block_number)
            if A == -1:
              quit()
            for i in range(BLOCK_SIZE):
              result[i] = result[i] ^ A[i]
    except ConnectionRefusedError:
      print("SERVER_DISCONNECTED GET " + str(server_id))
      if server_id in self.block_server:
        self.block_server.pop(server_id)
      result = self.SingleGet((server_id + 1) % self.NS, block_number)
      #print("server_id is " + str(server_id))
      if result == -1:
        quit()
      for other_server_id in range(0, self.NS):
        if (other_server_id != server_id) and (other_server_id != ((server_id + 1) % self.NS)):
          #print(other_server_id)
          #print(bool(other_server_id != ((server_id + 1) % self.NS)))
          #print(bool(other_server_id != server_id))
          A = self.SingleGet(other_server_id, block_number)
          if A == -1:
            quit()
          for i in range(BLOCK_SIZE):
            result[i] = result[i] ^ A[i]
    return result

  def SingleGet(self, server_id, block_number):

    logging.debug ('Get: ' + str(block_number))
    if block_number in range(0,TOTAL_NUM_BLOCKS):
      # logging.debug ('\n' + str((self.block[block_number]).hex()))
      # commenting this out as the request now goes to the server
      # return self.block[block_number]
      # call Get() method on the server
      data = self.block_server[server_id].Get(block_number)
      if data == -1:
        return data
      # return as bytearray
      return bytearray(data)

    logging.error('Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
    quit()

## RSM: read and set memory equivalent

  def RSM(self, block_number):
    try:
      result = self.SingleRSM(0, block_number)
      if result == -1:
        print("CORRUPTED_BLOCK " + str(block_number))
        # deal with this in RAID
        result = self.SingleRSM(1, block_number)
    except ConnectionRefusedError:
      print("SERVER_DISCONNECTED RSM " + str(0))
      quit()
    return result

  def SingleRSM(self, server_id, block_number):

    logging.debug ('RSM: ' + str(block_number))
    if block_number in range(0,TOTAL_NUM_BLOCKS):
      data = self.block_server[server_id].RSM(block_number)
      if data == -1:
        return data
      return bytearray(data)

    logging.error('RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
    quit()

  def Repair(self, server_id):

    logging.debug('Repair: ' + str(server_id))
    if server_id not in range(self.NS):
      return -1, "SERVER_OUT_OF_RANGE"
    #if server_id in self.block_server:
      #print("REPAIR MAY NOT BE REQUIRED - REPAIRING")
      #return -1, "REPAIR_NOT_REQUIRED"
    server_url = 'http://' + SERVER_ADDRESS + ':' + str(self.STARTPORT + server_id)
    self.block_server[server_id] = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)
    # maybe add try - catch to stop on disconnect
    #print("Total num blocks: " + str(TOTAL_NUM_BLOCKS))
    #print("NS: " + str(self.NS))
    #print(type(TOTAL_NUM_BLOCKS/(self.NS - 1)))
    for block_number in range(0, TOTAL_NUM_BLOCKS // (self.NS - 1)):
      new_block = bytearray(BLOCK_SIZE)
      for other_server_id in range(0, self.NS):
        if other_server_id != server_id:
          A = self.SingleGet(other_server_id, block_number)
          for i in range(BLOCK_SIZE):
            new_block[i] = new_block[i] ^ A[i]
      self.SinglePut(server_id, block_number, new_block)

    return 0, "SUCCESS"

## Acquire and Release using a disk block lock

  def Acquire(self):
    # commenting out code
    logging.debug ('Acquire')
    #lockvalue = self.RSM(RSM_BLOCK);
    #logging.debug ("RSM_BLOCK Lock value: " + str(lockvalue))
    #while lockvalue[0] == 1: # test just first byte of block to check if RSM_LOCKED
    #  logging.debug ("Acquire: spinning...")
    #  lockvalue = self.RSM(RSM_BLOCK);
    return 0

  def Release(self):
    # commenting out code
    logging.debug ('Release')
    # Put()s a zero-filled block to release lock
    #self.Put(RSM_BLOCK,bytearray(RSM_UNLOCKED.ljust(BLOCK_SIZE,b'\x00')))
    return 0


## Serializes and saves block[] data structure to a disk file

  def DumpToDisk(self, filename):

    logging.info("Dumping pickled blocks to file " + filename)
    file = open(filename,'wb')
    file_system_constants = "BS_" + str(BLOCK_SIZE) + "_NB_" + str(TOTAL_NUM_BLOCKS) + "_IS_" + str(INODE_SIZE) \
                            + "_MI_" + str(MAX_NUM_INODES) + "_MF_" + str(MAX_FILENAME) + "_IDS_" + str(INODE_NUMBER_DIRENTRY_SIZE)
    pickle.dump(file_system_constants, file)
    pickle.dump(self.block, file)

    file.close()


## Loads block[] data structure from a disk file

  def LoadFromDisk(self, filename):

    logging.info("Reading blocks from pickled file " + filename)
    file = open(filename,'rb')
    file_system_constants = "BS_" + str(BLOCK_SIZE) + "_NB_" + str(TOTAL_NUM_BLOCKS) + "_IS_" + str(INODE_SIZE) \
                            + "_MI_" + str(MAX_NUM_INODES) + "_MF_" + str(MAX_FILENAME) + "_IDS_" + str(
      INODE_NUMBER_DIRENTRY_SIZE)

    try:
      read_file_system_constants = pickle.load(file)

      if file_system_constants != read_file_system_constants:
        print('Error: File System constants of File :' + read_file_system_constants
            + ' do not match with current file system constants :' + file_system_constants)
        return -1

      block = pickle.load(file)
      for i in range(0, TOTAL_NUM_BLOCKS):
        self.Put(i,block[i])
      return 0
    except TypeError:
      print("Error: File not in proper format, encountered type error ")
      return -1
    except EOFError:
      print("Error: File not in proper format, encountered EOFError error ")
      return -1
    finally:
      file.close()


## Initialize blocks, either from a clean slate (cleanslate == True), or from a pickled dump file with prefix
    
  def InitializeBlocks(self, prefix):

    # Block 0: No real boot code here, just write the given prefix
    self.Put(0,prefix)

    # Block 1: Superblock contains basic file system constants
    # First, we write it as a list
    superblock = [TOTAL_NUM_BLOCKS, BLOCK_SIZE, MAX_NUM_INODES, INODE_SIZE]
    # Now we serialize it into a byte array
    self.Put(1,pickle.dumps(superblock))

    # Blocks 2-TOTAL_NUM_BLOCKS are initialized with zeroes
    #   Free block bitmap: All blocks start free, so safe to initialize with zeroes
    #   Inode table: zero indicates an invalid inode, so also safe to initialize with zeroes
    #   Data blocks: safe to init with zeroes
    zeroblock = bytearray(BLOCK_SIZE)
    for i in range(FREEBITMAP_BLOCK_OFFSET, TOTAL_NUM_BLOCKS):
      self.Put(i, zeroblock)


## Prints out file system information

  def PrintFSInfo(self):
    logging.info ('#### File system information:')
    logging.info ('Number of blocks          : ' + str(TOTAL_NUM_BLOCKS))
    logging.info ('Block size (Bytes)        : ' + str(BLOCK_SIZE))
    logging.info ('Number of inodes          : ' + str(MAX_NUM_INODES))
    logging.info ('inode size (Bytes)        : ' + str(INODE_SIZE))
    logging.info ('inodes per block          : ' + str(INODES_PER_BLOCK))
    logging.info ('Free bitmap offset        : ' + str(FREEBITMAP_BLOCK_OFFSET))
    logging.info ('Free bitmap size (blocks) : ' + str(FREEBITMAP_NUM_BLOCKS))
    logging.info ('Inode table offset        : ' + str(INODE_BLOCK_OFFSET))
    logging.info ('Inode table size (blocks) : ' + str(INODE_NUM_BLOCKS))
    logging.info ('Max blocks per file       : ' + str(MAX_INODE_BLOCK_NUMBERS))
    logging.info ('Data blocks offset        : ' + str(DATA_BLOCKS_OFFSET))
    logging.info ('Data block size (blocks)  : ' + str(DATA_NUM_BLOCKS))
    logging.info ('Raw block layer layout: (B: boot, S: superblock, F: free bitmap, I: inode, D: data')
    Layout = "BS"
    Id = "01"
    IdCount = 2
    for i in range(0,FREEBITMAP_NUM_BLOCKS):
      Layout += "F"
      Id += str(IdCount)
      IdCount = (IdCount + 1) % 10
    for i in range(0,INODE_NUM_BLOCKS):
      Layout += "I"
      Id += str(IdCount)
      IdCount = (IdCount + 1) % 10
    for i in range(0,DATA_NUM_BLOCKS):
      Layout += "D"
      Id += str(IdCount)
      IdCount = (IdCount + 1) % 10
    logging.info (Id)
    logging.info (Layout)


## Prints to screen block contents, from min to max

  def PrintBlocks(self,tag,min,max):
    logging.info ('#### Raw disk blocks: ' + tag)
    for i in range(min,max):
      logging.info ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))



#### INODE LAYER 


# This class holds an inode object in memory and provides methods to modify inodes
# The pattern here is:
#  0. Initialize the object
#  1. Read an Inode object from a byte array read from raw block storage (InodeFromBytearray)
#     An inode is stored in a raw block as a byte array:
#       size (bytes 0..3), type (bytes 4..5), refcnt (bytes 6..7), block_numbers (bytes 8..)
#  2. Update inode (e.g. size, refcnt, block numbers) depending on file system operation
#     Using various Set() methods
#  3. Serialize and write Inode object back to raw block storage (InodeToBytearray)

class Inode():
  def __init__(self):

    # inode is initialized empty
    self.type = INODE_TYPE_INVALID
    self.size = 0
    self.refcnt = 0
    self.gencnt = 0

    # We store inode block_numbers as a list
    self.block_numbers = []

    # initialize list with zeroes
    for i in range(0,MAX_INODE_BLOCK_NUMBERS):
      self.block_numbers.append(0)


## Set inode object values from a raw bytearray
## This is used when we read an inode from raw block storage to the inode data structure

  def InodeFromBytearray(self,b):

    if len(b) > INODE_SIZE:
      logging.error ('InodeFromBytearray: exceeds inode size ' + str(b))
      quit()

    # slice the raw bytes for the different fields
    # size is 4 bytes, type 2 bytes, refcnt 2 bytes
    size_slice = b[0:4]
    type_slice = b[4:6]
    refcnt_slice = b[6:8]

    # converts from raw bytes to integers using big-endian 
    # store scalars 
    self.size = int.from_bytes(size_slice, byteorder='big') 
    self.type = int.from_bytes(type_slice, byteorder='big') 
    self.refcnt = int.from_bytes(refcnt_slice, byteorder='big')
    gencnt_slice = b[8:12]
    self.gencnt = int.from_bytes(gencnt_slice, byteorder='big')

    # each block number entry is 4 bytes, big-endian
    for i in range(0,MAX_INODE_BLOCK_NUMBERS):
      start = INODE_METADATA_SIZE + i*4

      blocknumber_slice = b[start:start+4]
      self.block_numbers[i] = int.from_bytes(blocknumber_slice, byteorder='big') 


## Create a raw byte array, serializing Inode object values to prepare to write
## This is used when we write an inode to raw block storage

  def InodeToBytearray(self):

    # Temporary bytearray - we'll load it with the different inode fields
    temparray = bytearray(INODE_SIZE)

    # We assume size is 4 bytes, and we store it in Big Endian format
    intsize = self.size
    temparray[0:4] = intsize.to_bytes(4, 'big')

    # We assume type is 2 bytes, and we store it in Big Endian format
    inttype = self.type
    temparray[4:6] = inttype.to_bytes(2, 'big')

    # We assume refcnt is 2 bytes, and we store it in Big Endian format
    intrefcnt = self.refcnt
    temparray[6:8] = intrefcnt.to_bytes(2, 'big')

    # We assume gencnt is 4 bytes, and we store it in Big Endian format
    intgencnt = self.gencnt
    temparray[8:12] = intgencnt.to_bytes(4, 'big')

    # We assume each block number is 4 bytes, and we store each in Big Endian format
    for i in range(0,MAX_INODE_BLOCK_NUMBERS):
      start = INODE_METADATA_SIZE + i*4
      intbn = self.block_numbers[i]
      temparray[start:start+4] = intbn.to_bytes(4, 'big')

    # Return the byte array
    return temparray


## Prints out this inode object's information to the log

  def Print(self):
    logging.info ('Inode size   : ' + str(self.size))
    logging.info ('Inode type   : ' + str(self.type))
    logging.info ('Inode refcnt : ' + str(self.refcnt))
    logging.info ('Inode gencnt : ' + str(self.gencnt))
    logging.info ('Block numbers: ')
    s = ""
    for i in range(0,MAX_INODE_BLOCK_NUMBERS):
      s += str(self.block_numbers[i])
      s += ","
    logging.info (s)


#### Inode number layer


class InodeNumber():
  def __init__(self, RawBlocks, number):

    # This object stores the inode data structure
    self.inode = Inode()

    # This stores the inode number
    if number > MAX_NUM_INODES:
      logging.error ('InodeNumber: inode number exceeds limit: ' + str(number))
      quit()
    self.inode_number = number

    # Raw block storage
    self.RawBlocks = RawBlocks


## Load inode data structure from raw storage, indexed by inode number
## The inode data structure loaded from raw storage goes in the self.inode object

  def InodeNumberToInode(self):

    logging.debug('InodeNumberToInode: ' + str(self.inode_number))

    # locate which block has the inode we want
    raw_block_number = INODE_BLOCK_OFFSET + ((self.inode_number * INODE_SIZE) // BLOCK_SIZE)

    # Get the entire block containing inode from raw storage
    tempblock = self.RawBlocks.Get(raw_block_number)

    # Find the slice of the block for this inode_number
    start = (self.inode_number * INODE_SIZE) % BLOCK_SIZE
    end = start + INODE_SIZE

    # extract byte array for this inode
    tempinode = tempblock[start:end]

    # load inode from byte array 
    self.inode.InodeFromBytearray(tempinode)

    logging.debug ('InodeNumberToInode : inode_number ' + str(self.inode_number) + ' raw_block_number: ' + str(raw_block_number) + ' slice start: ' + str(start) + ' end: ' + str(end))
    logging.debug ('tempinode: ' + str(tempinode.hex()))


## Stores (Put) this inode into raw storage
## Since an inode is a slice of a block, we first Get() the block, update the slice, and Put()

  def StoreInode(self):

    logging.debug('StoreInode: ' + str(self.inode_number))

    # locate which block has the inode we want
    raw_block_number = INODE_BLOCK_OFFSET + ((self.inode_number * INODE_SIZE) // BLOCK_SIZE)
    logging.debug('StoreInode: raw_block_number ' + str(raw_block_number))

    # Get the entire block containing inode from raw storage
    tempblock = self.RawBlocks.Get(raw_block_number)
    logging.debug('StoreInode: tempblock:\n' + str(tempblock.hex()))

    # Find the slice of the block for this inode_number
    start = (self.inode_number * INODE_SIZE) % BLOCK_SIZE
    end = start + INODE_SIZE
    logging.debug('StoreInode: start: ' + str(start) + ', end: ' + str(end))

    # serialize inode into byte array
    inode_bytearray = self.inode.InodeToBytearray()

    # Update slice of block with this inode's bytearray
    tempblock[start:end] = inode_bytearray     
    logging.debug('StoreInode: tempblock:\n' + str(tempblock.hex()))

    # Update raw storage with new inode
    self.RawBlocks.Put(raw_block_number, tempblock)


## Returns a block of data from raw storage, given its offset
## Equivalent to textbook's INODE_NUMBER_TO_BLOCK

  def InodeNumberToBlock(self, offset):

    logging.debug ('InodeNumberToBlock: ' + str(offset))

    # Load object's inode 
    self.InodeNumberToInode()

    # Calculate offset
    o = offset // BLOCK_SIZE

    # Retrieve block indexed by offset
    # as in the textbook's INDEX_TO_BLOCK_NUMBER
    b = self.inode.block_numbers[o] 
    block = self.RawBlocks.Get(b)
    return block


#### File name layer


## This class implements methods for the file name layer

class FileName():
  def __init__(self, RawBlocks):
    self.RawBlocks = RawBlocks


## This helper function extracts a file name string from a directory data block
## The index selects which file name entry to extract within the block - e.g. index 0 is the first file name, 1 second file name

  def HelperGetFilenameString(self, block, index):

    logging.debug ('HelperGetFilenameString: ' + str(block.hex()) + ', ' + str(index))

    # Locate bytes that store string - first MAX_FILENAME characters aligned by MAX_FILENAME + INODE_NUMBER_DIRENTRY_SIZE
    string_start = index * FILE_NAME_DIRENTRY_SIZE
    string_end = string_start + MAX_FILENAME

    return block[string_start:string_end]


## This helper function extracts an inode number from a directory data block
## The index selects which entry to extract within the block - e.g. index 0 is the inode for the first file name, 1 second file name

  def HelperGetFilenameInodeNumber(self, block, index):

    logging.debug ('HelperGetFilenameInodeNumber: ' + str(block.hex()) + ', ' + str(index))

    # Locate bytes that store inode
    inode_start = (index * FILE_NAME_DIRENTRY_SIZE) + MAX_FILENAME
    inode_end = inode_start + INODE_NUMBER_DIRENTRY_SIZE
    inodenumber_slice = block[inode_start:inode_end]

    return int.from_bytes(inodenumber_slice, byteorder='big')


## This inserts a (filename,inodenumber) entry into the tail end of the table in a directory data block of insert_into InodeNumber()
## Used when adding an entry to a directory
## insert_into is an InodeNumber() object; filename is a string; inodenumber is an integer

  def InsertFilenameInodeNumber(self, insert_to, filename, inodenumber):

    logging.debug ('InsertFilenameInodeNumber: ' + str(filename) + ', ' + str(inodenumber))

    if len(filename) > MAX_FILENAME:
      logging.error ('InsertFilenameInodeNumber: file name exceeds maximum')
      quit()

    if insert_to.inode.type != INODE_TYPE_DIR:
      logging.error ('InsertFilenameInodeNumber: not a directory inode: ' + str(insert_to.inode.type))
      quit()

    # We insert a new entry at the end of the existing table, so determine its position based on inode's size
    index = insert_to.inode.size
    if index >= MAX_FILE_SIZE:
      logging.error ('InsertFilenameInodeNumber: no space for another entry in inode')
      quit()

    # Check if we need to allocate another data block for this inode
    # this happens when the index spills over to the next block; index == 0 is a special case as an inode is
    # initialized with one data block, so no need to allocate
    block_number_index = index // BLOCK_SIZE
    if index % BLOCK_SIZE == 0:
      if index != 0:
        # Allocate the block
        new_block = self.AllocateDataBlock()
        # update inode (it will be written to raw storage before the method returns)
        insert_to.inode.block_numbers[block_number_index] = new_block
      
    # Retrieve the data block where the new (filename,inodenumber) will be stored
    block_number = insert_to.inode.block_numbers[block_number_index]
    block = self.RawBlocks.Get(block_number)

    # Compute module of index to locate entry within block
    index_modulo = index % BLOCK_SIZE

    # Locate the byte slice holding the file name with MAX_FILENAME size
    string_start = index_modulo 
    string_end = string_start + MAX_FILENAME
    stringbyte = bytearray(filename,"utf-8")

    # Locate the byte slice holding the inode number with INODE_NUMBER_DIRENTRY_SIZE size
    inode_start = index_modulo + MAX_FILENAME
    inode_end = inode_start + INODE_NUMBER_DIRENTRY_SIZE

    logging.debug ('InsertFilenameInodeNumber: \n' + str(block.hex()))
    logging.debug ('InsertFilenameInodeNumber: inode_start ' + str(inode_start) + ', inode_end ' + str(inode_end))
    logging.debug ('InsertFilenameInodeNumber: string_start ' + str(string_start) + ', string_end ' + str(string_end))

    # Update and write data block with (filename,inode) mapping
    block[inode_start:inode_end] = inodenumber.to_bytes(INODE_NUMBER_DIRENTRY_SIZE, 'big')
    block[string_start:string_end] = bytearray(stringbyte.ljust(MAX_FILENAME,b'\x00'))
    self.RawBlocks.Put(block_number, block)

    # Increment size, and write inode
    insert_to.inode.size += FILE_NAME_DIRENTRY_SIZE
    insert_to.StoreInode()

## Lookup string filename in the context of inode dir - same as textbook's LOOKUP

  def Lookup(self, filename, dir):

    logging.debug ('Lookup: ' + str(filename) + ', ' + str(dir))

    # Initialize inode_number object from raw storage
    inode_number = InodeNumber(self.RawBlocks, dir)
    inode_number.InodeNumberToInode()

    if inode_number.inode.type != INODE_TYPE_DIR:
      logging.error ("Lookup: not a directory inode: " + str(dir) + " , " + str(inode_number.inode.type))
      return -1

    offset = 0
    scanned = 0

    # Iterate over all data blocks indexed by directory inode, until we reach inode's size
    while offset < inode_number.inode.size:

      # Retrieve directory data block given current offset
      b = inode_number.InodeNumberToBlock(offset)

      # A directory data block has multiple (filename,inode) entries 
      # Iterate over file entries to search for matches
      for i in range (0, FILE_ENTRIES_PER_DATA_BLOCK):

        # don't search beyond file size
        if inode_number.inode.size > scanned:

          scanned += FILE_NAME_DIRENTRY_SIZE
 
          # Extract padded MAX_FILENAME string as a bytearray from data block for comparison
          filestring = self.HelperGetFilenameString(b,i)

          logging.debug ("Lookup for " + filename + " in " + str(dir) + ": searching string " + str(filestring))

          # Pad filename with zeroes and make it a byte array
          padded_filename = bytearray(filename,"utf-8")
          padded_filename = bytearray(padded_filename.ljust(MAX_FILENAME,b'\x00'))

          # these are now two byte arrays of the same MAX_FILENAME size, ready for comparison 
          if filestring == padded_filename:

            # On a match, retrieve and return inode number
            fileinode = self.HelperGetFilenameInodeNumber(b,i)
            logging.debug ("Lookup successful: " + str(fileinode))
            return fileinode

      # Skip to the next block, back to while loop
      offset += BLOCK_SIZE

    logging.debug ("Lookup: file not found: " + str(filename) + " in " + str(dir))
    return -1


## Scans inode table to find an available entry

  def FindAvailableInode(self):

    logging.debug ('FindAvailableInode: ')

    for i in range (0, MAX_NUM_INODES):

      # Initialize inode_number object from raw storage
      inode_number = InodeNumber(self.RawBlocks, i)
      inode_number.InodeNumberToInode()

      if inode_number.inode.type == INODE_TYPE_INVALID:
        logging.debug ("FindAvailableInode: " + str(i))
        return i

    logging.debug ("FindAvailableInode: no available inodes")
    return -1


## Returns index to an available entry in directory data block

  def FindAvailableFileEntry(self, dir):

    logging.debug ('FindAvailableFileEntry: dir: ' + str(dir))

    # Initialize inode_number object from raw storage
    inode_number = InodeNumber(self.RawBlocks, dir)
    inode_number.InodeNumberToInode()

    # Check if there is still room for another (filename,inode) entry
    # the inode cannot exceed maximum size 
    if inode_number.inode.size >= MAX_FILE_SIZE:
      logging.debug ("FindAvailableFileEntry: no entries available")
      return -1

    logging.debug ("FindAvailableFileEntry: " + str(inode_number.inode.size))
    return inode_number.inode.size


## Allocate a data block, update free bitmap, and return its number

  def AllocateDataBlock(self):

    logging.debug ('AllocateDataBlock: ')

    # Scan through all available data blocks
    for block_number in range (DATA_BLOCKS_OFFSET, TOTAL_NUM_BLOCKS):

      # GET() raw block that stores the bitmap entry for block_number
      bitmap_block = FREEBITMAP_BLOCK_OFFSET + (block_number // BLOCK_SIZE)
      block = self.RawBlocks.Get(bitmap_block)

      # Locate proper byte within the block
      byte_bitmap = block[block_number % BLOCK_SIZE]

      # Data block block_number is free
      if byte_bitmap == 0:
        # Mark it as used in bitmap
        block[block_number % BLOCK_SIZE] = 1
        self.RawBlocks.Put(bitmap_block, block)
        logging.debug ('AllocateDataBlock: allocated ' + str(block_number))
        return block_number

    logging.debug ('AllocateDataBlock: no free data blocks available')
    quit()


## Initializes the root inode

  def InitRootInode(self):

    # Root inode has well-known value 0
    root_inode = InodeNumber(self.RawBlocks, 0)
    root_inode.InodeNumberToInode()
    root_inode.inode.type = INODE_TYPE_DIR
    root_inode.inode.size = 0
    root_inode.inode.refcnt = 1
    # Allocate one data block and set as first entry in block_numbers[]
    root_inode.inode.block_numbers[0] = self.AllocateDataBlock()
    # Add "." 
    self.InsertFilenameInodeNumber(root_inode, ".", 0)
    root_inode.inode.Print()
    root_inode.StoreInode()


## Create a file system object
## type determines the type of file system object to be created
## dir is the inode number of a directory to hold the object
## name is the object's name

  def Create(self, dir, name, type):

    logging.debug ("Create: dir: " + str(dir) + ", name: " + str(name) + ", type: " + str(type))

    # Ensure type is valid
    if not (type == INODE_TYPE_FILE or type == INODE_TYPE_DIR):
      logging.debug ("ERROR_CREATE_INVALID_TYPE " + str(type))
      return -1, "ERROR_CREATE_INVALID_TYPE"

    # Find if there is an available inode
    inode_position = self.FindAvailableInode()
    if inode_position == -1:
      logging.debug ("ERROR_CREATE_INODE_NOT_AVAILABLE")
      return -1, "ERROR_CREATE_INODE_NOT_AVAILABLE"

    # Obtain dir_inode_number_inode, ensure it is a directory
    dir_inode = InodeNumber(self.RawBlocks, dir)
    dir_inode.InodeNumberToInode()
    if dir_inode.inode.type != INODE_TYPE_DIR:
      logging.debug ("ERROR_CREATE_INVALID_DIR " + str(dir))
      return -1, "ERROR_CREATE_INVALID_DIR"

    # Find available slot in directory data block
    fileentry_position = self.FindAvailableFileEntry(dir)
    if fileentry_position == -1:
      logging.debug ("ERROR_CREATE_DATA_BLOCK_NOT_AVAILABLE")
      return -1, "ERROR_CREATE_DATA_BLOCK_NOT_AVAILABLE"

    # Ensure it's not a duplicate - if Lookup returns anything other than -1
    if self.Lookup(name, dir) != -1:
      logging.debug ("ERROR_CREATE_ALREADY_EXISTS " + str(name))
      return -1, "ERROR_CREATE_ALREADY_EXISTS"

    logging.debug ("Create: inode_position: " + str(inode_position) + ", fileentry_position: " + str(fileentry_position))

    if type == INODE_TYPE_DIR:
      # Store inode of new directory
      newdir_inode = InodeNumber(self.RawBlocks, inode_position)
      newdir_inode.InodeNumberToInode()
      newdir_inode.inode.type = INODE_TYPE_DIR
      newdir_inode.inode.size = 0
      newdir_inode.inode.refcnt = 1
      # Allocate one data block and set as first entry in block_numbers[]
      newdir_inode.inode.block_numbers[0] = self.AllocateDataBlock()
      newdir_inode.StoreInode()

      # Add to directory (filename,inode) table
      self.InsertFilenameInodeNumber(dir_inode, name, inode_position)

      # Add "." to new directory
      self.InsertFilenameInodeNumber(newdir_inode, ".", inode_position)

      # Add ".." to new directory
      self.InsertFilenameInodeNumber(newdir_inode, "..", dir)

      # Update directory inode
      # increment refcnt
      dir_inode.inode.refcnt += 1
      dir_inode.StoreInode()

    elif type == INODE_TYPE_FILE:
      newfile_inode = InodeNumber(self.RawBlocks, inode_position)
      newfile_inode.InodeNumberToInode()
      newfile_inode.inode.type = INODE_TYPE_FILE
      newfile_inode.inode.size = 0
      newfile_inode.inode.refcnt = 1
      # New files are not allocated any blocks; these are allocated on a Write()
      newfile_inode.StoreInode()

      # Add to parent's (filename,inode) table
      self.InsertFilenameInodeNumber(dir_inode, name, inode_position) 
    
      # Update directory inode
      # refcnt incremented by one
      dir_inode.inode.refcnt += 1
      dir_inode.StoreInode()

    # Return new object's inode number
    return inode_position, "SUCCESS"


## Writes data to a file, starting at offset
## offset must be less than or equal to the file's size
## data is a block array
## returns number of bytes written

  def Write(self, file_inode_number, offset, data):

    logging.debug ("Write: file_inode_number: " + str(file_inode_number) + ", offset: " + str(offset) + ", len(data): " + str(len(data)))
    # logging.debug (str(data))

    file_inode = InodeNumber(self.RawBlocks, file_inode_number)
    file_inode.InodeNumberToInode()

    if file_inode.inode.type != INODE_TYPE_FILE:
      logging.debug ("ERROR_WRITE_NOT_FILE " + str(file_inode_number))
      return -1, "ERROR_WRITE_NOT_FILE"

    if offset > file_inode.inode.size:
      logging.debug ("ERROR_WRITE_OFFSET_LARGER_THAN_SIZE " + str(offset))
      return -1, "ERROR_WRITE_OFFSET_LARGER_THAN_SIZE"

    if offset + len(data) > MAX_FILE_SIZE:
      logging.debug ("ERROR_WRITE_EXCEEDS_FILE_SIZE " + str(offset + len(data)))
      return -1, "ERROR_WRITE_EXCEEDS_FILE_SIZE"

    # initialize variables used in the while loop
    current_offset = offset
    bytes_written = 0

    # this loop iterates through one or more blocks, ending when all data is written
    while bytes_written < len(data):

      # block index corresponding to the current offset
      current_block_index = current_offset // BLOCK_SIZE

      # next block's boundary (in Bytes relative to file 0)
      next_block_boundary = (current_block_index + 1) * BLOCK_SIZE

      logging.debug ('Write: current_block_index: ' + str(current_block_index) + ' , next_block_boundary: ' + str(next_block_boundary))

      # byte position where the slice of data to write should start, within a block
      # the first time around in the loop, this may not be aligned with block boundary (i.e. 0) depending on offset
      # in subsequent iterations, it will be 0
      write_start = current_offset % BLOCK_SIZE

      # determine byte position where the writing ends
      # this may be BLOCK_SIZE if the data yet to be written spills over to the next block
      # or, it may be smaller than BLOCK_SIZE if the data ends in this block

      if (offset + len(data)) >= next_block_boundary:
        # the data length is such that it goes beyond this block, so we're writing this entire block
        write_end = BLOCK_SIZE
      else:
        # otherwise, the data is truncated within this block
        write_end = (offset + len(data)) % BLOCK_SIZE

      logging.debug ('Write: write_start: ' + str(write_start) + ' , write_end: ' + str(write_end))
        
      # retrieve index of block to be written from inode's list
      block_number = file_inode.inode.block_numbers[current_block_index]

      # if the block is not allocated, allocate
      if block_number == 0:
        new_block = self.AllocateDataBlock()
        # update inode (it will be written to raw storage before the method returns)
        file_inode.inode.block_numbers[current_block_index] = new_block
        block_number = new_block

      # first, we read the whole block from raw storage
      block = file_inode.RawBlocks.Get(block_number)

      # copy slice of data into the right position in the block
      block[write_start:write_end] = data[bytes_written:bytes_written+(write_end-write_start)]
            
      # now write modified block back to disk
      file_inode.RawBlocks.Put(block_number,block)

      # update offset, bytes written
      current_offset += write_end-write_start
      bytes_written += write_end-write_start

      logging.debug ('Write: current_offset: ' + str(current_offset) + ' , bytes_written: ' + str(bytes_written) + ' , len(data): ' + str(len(data)))

    # Update inode's metadata and write to storage
    file_inode.inode.size += bytes_written
    file_inode.StoreInode()

    return bytes_written, "SUCCESS"



  def Read(self, file_inode_number, offset, count):

    logging.debug ("Read: file_inode_number: " + str(file_inode_number) + ", offset: " + str(offset) + ", count: " + str(count))

    file_inode = InodeNumber(self.RawBlocks, file_inode_number)
    file_inode.InodeNumberToInode()

    if file_inode.inode.type != INODE_TYPE_FILE:
      logging.debug ("ERROR_READ_NOT_FILE " + str(file_inode_number))
      return -1, "ERROR_READ_NOT_FILE"

    if offset > file_inode.inode.size:
      logging.debug ("ERROR_READ_OFFSET_LARGER_THAN_SIZE " + str(offset))
      return -1, "ERROR_READ_OFFSET_LARGER_THAN_SIZE"

    # initialize variables used in the while loop
    current_offset = offset
    bytes_read = 0

    if offset + count > file_inode.inode.size:
      bytes_to_read = file_inode.inode.size - offset
    else:
      bytes_to_read = count

    read_block = bytearray(bytes_to_read)

    # this loop iterates through one or more blocks, ending when all data is read
    while bytes_read < bytes_to_read:

      # block index corresponding to the current offset
      current_block_index = current_offset // BLOCK_SIZE

      # next block's boundary (in Bytes relative to file 0)
      next_block_boundary = (current_block_index + 1) * BLOCK_SIZE

      logging.debug ('Read: current_block_index: ' + str(current_block_index) + ' , next_block_boundary: ' + str(next_block_boundary))

      read_start = current_offset % BLOCK_SIZE

      if (offset + bytes_to_read) >= next_block_boundary:
        # the data length is such that it goes beyond this block, so we're reading this entire block
        read_end = BLOCK_SIZE
      else:
        # otherwise, the data is truncated within this block
        read_end = (offset + bytes_to_read) % BLOCK_SIZE

      logging.debug ('Read: read_start: ' + str(read_start) + ' , read_end: ' + str(read_end))

      # retrieve index of block to be written from inode's list
      block_number = file_inode.inode.block_numbers[current_block_index]

      # first, we read the whole block from raw storage
      block = file_inode.RawBlocks.Get(block_number)

      # copy slice of data into the right position in the block
      read_block[bytes_read:bytes_read+(read_end-read_start)] = block[read_start:read_end]

      bytes_read += read_end-read_start
      current_offset += read_end-read_start

      logging.debug ('Read: current_offset: ' + str(current_offset) + ' , bytes_read: ' + str(bytes_read))

    return read_block, "SUCCESS"


## Unlink a file 
## dir is the inode number of current working directory 
## name is the file's name

  def Unlink(self, dir, name):

    logging.debug ("Unlink: dir: " + str(dir) + ", name: " + str(name))

    # Obtain dir_inode_number_inode, ensure it is a directory
    dir_inode = InodeNumber(self.RawBlocks, dir)
    dir_inode.InodeNumberToInode()
    if dir_inode.inode.type != INODE_TYPE_DIR:
      logging.debug ("ERROR_UNLINK_INVALID_DIR " + str(dir))
      return -1, "ERROR_UNLINK_INVALID_DIR"

    # Ensure file exists - if Lookup returns -1 it does not exist
    file_inode = self.Lookup(name, dir)
    if file_inode == -1:
      logging.debug ("ERROR_UNLINK_DOESNOT_EXIST " + str(name))
      return -1, "ERROR_UNLINK_DOESNOT_EXIST"

    # Ensure it is a regular file or symlink
    file = InodeNumber(self.RawBlocks, file_inode)
    file.InodeNumberToInode()
    # if file.inode.type != INODE_TYPE_FILE:
    if file.inode.type == INODE_TYPE_DIR:
      logging.debug ("ERROR_UNLINK_NOT_FILE " + str(name))
      return -1, "ERROR_UNLINK_NOT_FILE"

    # Remove binding of inode in directory
    # We need to search all directory entries until we find a match with inode number

    block_index = 0
    # Scan through all possible blocks in the directory
    while block_index <= (dir_inode.inode.size // BLOCK_SIZE):
      # Read block from disk
      block = self.RawBlocks.Get(dir_inode.inode.block_numbers[block_index])
      current_position = 0 
      # can starting from 0, but need to determine where to stop scanning for this block: end_position
      if block_index == (dir_inode.inode.size // BLOCK_SIZE):
        # if this is the last block, stop at the inode size boundary
        end_position = dir_inode.inode.size % BLOCK_SIZE
      else:
        # otherwise, stop at block boundary
        end_position = BLOCK_SIZE

      # Now scan within the block
      while current_position < end_position:
        # Retrieve bytearray slice with name for this binding
        entryname = block[current_position:current_position+MAX_FILENAME]
        entryname_padded = bytearray(entryname.ljust(MAX_FILENAME,b'\x00'))
        padded_filename = bytearray(name,"utf-8")
        padded_filename = bytearray(padded_filename.ljust(MAX_FILENAME,b'\x00'))
        if entryname_padded == padded_filename:
          # found the entry to be removed - inode matches
          # print("TBD: found entry at " + str(current_position) + " " + str(entryinodenumber) + " " + str(file_inode))
          # we'll move the entry from the end of the directory list here
          # index of last block in the directory's list of blocks holds the entry to be moved
          last_block_index = (dir_inode.inode.size // BLOCK_SIZE)
          # read last block from disk
          last_block = self.RawBlocks.Get(dir_inode.inode.block_numbers[last_block_index])
          last_block_end_position = (dir_inode.inode.size-FILE_NAME_DIRENTRY_SIZE) % BLOCK_SIZE 
          if (last_block_index == block_index) and (last_block_end_position == current_position):
            # the entry we're unlinking is the last one already; no need to copy
            logging.debug ("Unlink: removing last entry from directory")
            # print ("the entry we're unlinking is the last one already; no need to copy " + str(last_block_index) + ", " + str(block_index) + ", " + str(last_block_end_position) + ", " + str(current_position))
            # clears the entry out
            block[current_position:current_position+FILE_NAME_DIRENTRY_SIZE] = bytearray(FILE_NAME_DIRENTRY_SIZE)
          else:
            # the entry is not the last - so must copy the last 
            logging.debug ("Unlink: moving last entry from directory")
            # print ("the entry is not the last - so must copy the last "  + str(last_block_index) + ", " + str(block_index) + ", " + str(last_block_end_position) + ", " + str(current_position))
            # copy from last
            block[current_position:current_position+FILE_NAME_DIRENTRY_SIZE] = last_block[last_block_end_position:last_block_end_position+FILE_NAME_DIRENTRY_SIZE]
            last_block[last_block_end_position:last_block_end_position+FILE_NAME_DIRENTRY_SIZE] = bytearray(FILE_NAME_DIRENTRY_SIZE)
            self.RawBlocks.Put(dir_inode.inode.block_numbers[last_block_index], last_block)
          # write the block back
          self.RawBlocks.Put(dir_inode.inode.block_numbers[block_index], block)
          # update inode size
          dir_inode.inode.size = dir_inode.inode.size - FILE_NAME_DIRENTRY_SIZE
          # decrement refcnt of directory
          dir_inode.inode.refcnt -= 1
          dir_inode.StoreInode()
          break
        current_position += FILE_NAME_DIRENTRY_SIZE
      block_index += 1


    # decrement refcnt
    file.inode.refcnt -= 1
    file.StoreInode()

    # Free inode/data block resources if it's the last reference
    if file.inode.refcnt == 0:
      logging.debug ("Unlink: last reference; freeing data blocks and inode")
 
      # Free data blocks
      # Scan all blocks in inode
      for i in range(0,MAX_INODE_BLOCK_NUMBERS):
        # if the block is allocated, free it up in bitmap
        block_number = file.inode.block_numbers[i]
        if block_number != 0:
          # print ("TBD: free block " + str(block_number))
          # index bitmap block
          bitmap_block = FREEBITMAP_BLOCK_OFFSET + (block_number // BLOCK_SIZE)
          # retrieve from disk
          block = self.RawBlocks.Get(bitmap_block)
          # Update bitmap to be 0 (free)
          block[block_number % BLOCK_SIZE] = 0
          # Write back to disk
          self.RawBlocks.Put(bitmap_block,block)


      # Free inode
      # Create new inode that is blank for the inode number
      new_blank_inode = InodeNumber(self.RawBlocks, file_inode)
      # store blank inode to disk
      new_blank_inode.StoreInode()

    return 0, "SUCCESS"


# Resolve a relative path to an inode number

  def PathToInodeNumber(self, path, dir):

    logging.debug ("PathToInodeNumber: path: " + str(path) + ", dir: " + str(dir))

    if "/" in path:
      split_path = path.split("/")
      first = split_path[0]
      del split_path[0]
      rest = "/".join(split_path)
      logging.debug ("PathToInodeNumber: first: " + str(first) + ", rest: " + str(rest))
      d = self.Lookup(first, dir)
      if d == -1:
        return -1
      return self.PathToInodeNumber(rest, d)
    else:
      return self.Lookup(path, dir)

# Resolve relative and absolute paths to an inode number

  def GeneralPathToInodeNumber(self, path, cwd):

    logging.debug ("GeneralPathToInodeNumber: path: " + str(path) + ", cwd: " + str(cwd))

    if path[0] == "/":
      if len(path) == 1: # special case: root
        logging.debug ("GeneralPathToInodeNumber: returning root inode 0")
        return 0 
      cut_path = path[1:len(path)]
      logging.debug ("GeneralPathToInodeNumber: cut_path: " + str(cut_path))
      return self.PathToInodeNumber(cut_path,0)
    else:
      return self.PathToInodeNumber(path,cwd)

  def GeneralPathToInodeNumber_Soft(self, path, cwd):

    # resolves soft links; see textbook p. 205

    logging.debug ("GeneralPathToInodeNumber_Soft: path: " + str(path) + ", cwd: " + str(cwd))

    i = self.GeneralPathToInodeNumber(path, cwd)
    lookedup_inode = InodeNumber(self.RawBlocks, i)
    lookedup_inode.InodeNumberToInode()

    if lookedup_inode.inode.type == INODE_TYPE_SYM:
      # print ("inode is symlink: " + str(i))
      # read block with target string from RawBlocks
      block_number = lookedup_inode.inode.block_numbers[0]
      block = lookedup_inode.RawBlocks.Get(block_number)
      # extract slice with length of target string
      target_slice = block[0:lookedup_inode.inode.size]
      rest = target_slice.decode()
      # print ("rest: " + rest)
      i = self.GeneralPathToInodeNumber(rest, cwd)

    return i

  def Link(self, target, name, cwd):

    logging.debug ("Link: target: " + str(target) + ", name: " + str(name) + ", cwd: " + str(cwd))

    target_inode_number = self.GeneralPathToInodeNumber(target, cwd)
    if target_inode_number == -1:
      logging.debug ("Link: target does not exist")
      return -1, "ERROR_LINK_TARGET_DOESNOT_EXIST"

    cwd_inode = InodeNumber(self.RawBlocks, cwd)
    cwd_inode.InodeNumberToInode()
    if cwd_inode.inode.type != INODE_TYPE_DIR:
      logging.debug ("Link: cwd is not a directory")
      return -1, "ERROR_LINK_NOT_DIRECTORY"

    # Find available slot in directory data block
    fileentry_position = self.FindAvailableFileEntry(cwd)
    if fileentry_position == -1:
      logging.debug ("Link: no entry available for another link")
      return -1, "ERROR_LINK_DATA_BLOCK_NOT_AVAILABLE"

    # Ensure it's not a duplicate - if Lookup returns anything other than -1
    if self.Lookup(name, cwd) != -1:
      logging.debug ("Link: name already exists")
      return -1, "ERROR_LINK_ALREADY_EXISTS"

    # Ensure target is a file 
    target_obj = InodeNumber(self.RawBlocks, target_inode_number)
    target_obj.InodeNumberToInode()
    if target_obj.inode.type != INODE_TYPE_FILE:
      logging.debug ("Link: target must be a file")
      return -1, "ERROR_LINK_TARGET_NOT_FILE"

    # Add to directory (filename,inode) table
    self.InsertFilenameInodeNumber(cwd_inode, name, target_inode_number)

    # Update refcnt of target and write to file system
    target_inode_number_object = InodeNumber(self.RawBlocks, target_inode_number)
    target_inode_number_object.InodeNumberToInode()
    target_inode_number_object.inode.refcnt += 1
    target_inode_number_object.StoreInode()

    # Update refcnt of directory and write to file system
    cwd_inode.inode.refcnt += 1
    cwd_inode.StoreInode()

    return 0, "SUCCESS"


  def Symlink(self, target, name, cwd):

    logging.debug ("Symlink: target: " + str(target) + ", name: " + str(name) + ", cwd: " + str(cwd))

    target_inode_number = self.GeneralPathToInodeNumber(target, cwd)
    if target_inode_number == -1:
      logging.debug ("Symlink: target does not exist")
      return -1, "ERROR_SYMLINK_TARGET_DOESNOT_EXIST"

    cwd_inode = InodeNumber(self.RawBlocks, cwd)
    cwd_inode.InodeNumberToInode()
    if cwd_inode.inode.type != INODE_TYPE_DIR:
      logging.debug ("Symlink: cwd is not a directory")
      return -1, "ERROR_SYMLINK_NOT_DIRECTORY"

    # Find available slot in directory data block
    fileentry_position = self.FindAvailableFileEntry(cwd)
    if fileentry_position == -1:
      logging.debug ("Symlink: no entry available for another link")
      return -1, "ERROR_SYMLINK_DATA_BLOCK_NOT_AVAILABLE"

    # Ensure it's not a duplicate - if Lookup returns anything other than -1
    if self.Lookup(name, cwd) != -1:
      logging.debug ("Symlink: name already exists")
      return -1, "ERROR_SYMLINK_ALREADY_EXISTS"

    # Find if there is an available inode
    inode_position = self.FindAvailableInode()
    if inode_position == -1:
      logging.debug ("ERROR_SYMLINK_INODE_NOT_AVAILABLE")
      return -1, "ERROR_SYMLINK_INODE_NOT_AVAILABLE"

    # ensure target size fits in a block
    if len(target) > BLOCK_SIZE:
      logging.debug ("ERROR_SYMLINK_TARGET_EXCEEDS_BLOCK_SIZE ")
      return -1, "ERROR_SYMLINK_TARGET_EXCEEDS_BLOCK_SIZE"

    # Create new Inode for symlink
    symlink_inode = InodeNumber(self.RawBlocks, inode_position)
    symlink_inode.InodeNumberToInode()
    symlink_inode.inode.type = INODE_TYPE_SYM
    symlink_inode.inode.size = len(target)
    symlink_inode.inode.refcnt = 1

    # Allocate one data block and set as first entry in block_numbers[]
    symlink_inode.inode.block_numbers[0] = self.AllocateDataBlock()
    symlink_inode.StoreInode()

    # Add to directory's (filename,inode) table
    self.InsertFilenameInodeNumber(cwd_inode, name, inode_position) 

    # Write target path to block
    # first, we read the whole block from raw storage
    block_number = symlink_inode.inode.block_numbers[0]
    block = symlink_inode.RawBlocks.Get(block_number)
    # copy slice of data into the right position in the block
    stringbyte = bytearray(target,"utf-8")
    block[0:len(target)] = stringbyte 
    # now write modified block back to disk
    symlink_inode.RawBlocks.Put(block_number,block)

    return 0, "SUCCESS"
