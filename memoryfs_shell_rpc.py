import pickle, logging
import argparse
import time

from memoryfs_client import *
import os.path

## This class implements an interactive shell to navigate the file system

class FSShell():
  def __init__(self, file):
    # cwd stored the inode of the current working directory
    # we start in the root directory
    self.cwd = 0
    self.FileObject = file

  # implements cd (change directory)
  def cd(self, dir):
    # i = self.FileObject.Lookup(dir,self.cwd)
    # now resolve a full path
    i = self.FileObject.GeneralPathToInodeNumber(dir,self.cwd)
    if i == -1:
      print ("Error: not found\n")
      return -1
    inobj = InodeNumber(self.FileObject.RawBlocks,i)
    inobj.InodeNumberToInode()
    if inobj.inode.type != INODE_TYPE_DIR:
      print ("Error: not a directory\n")
      return -1
    self.cwd = i

  # implements ls (lists files in directory)
  def ls(self):
    inobj = InodeNumber(self.FileObject.RawBlocks, self.cwd)
    inobj.InodeNumberToInode()
    block_index = 0
    while block_index <= (inobj.inode.size // BLOCK_SIZE):
      block = self.FileObject.RawBlocks.Get(inobj.inode.block_numbers[block_index])
      if block_index == (inobj.inode.size // BLOCK_SIZE):
        end_position = inobj.inode.size % BLOCK_SIZE
      else:
        end_position = BLOCK_SIZE
      current_position = 0
      while current_position < end_position:
        entryname = block[current_position:current_position+MAX_FILENAME]
        entryinode = block[current_position+MAX_FILENAME:current_position+FILE_NAME_DIRENTRY_SIZE]
        entryinodenumber = int.from_bytes(entryinode, byteorder='big')
        inobj2 = InodeNumber(self.FileObject.RawBlocks, entryinodenumber)
        inobj2.InodeNumberToInode()
        if inobj2.inode.type == INODE_TYPE_DIR:
          print ("[" + str(inobj2.inode.refcnt) + "]:" + entryname.decode() + "/")
        else:
          if inobj2.inode.type == INODE_TYPE_SYM:
            target_block_number = inobj2.inode.block_numbers[0]
            target_block = inobj2.RawBlocks.Get(target_block_number)
            target_slice = target_block[0:inobj2.inode.size]
            print ("[" + str(inobj2.inode.refcnt) + "]:" + entryname.decode() + "@ -> " + target_slice.decode())
          else:
            print ("[" + str(inobj2.inode.refcnt) + "]:" + entryname.decode())
        current_position += FILE_NAME_DIRENTRY_SIZE
      block_index += 1
    return 0

  # implements cat (print file contents)
  def cat(self, filename):
    # i = self.FileObject.Lookup(filename, self.cwd)
    # Now resolve full path
    i = self.FileObject.GeneralPathToInodeNumber_Soft(filename,self.cwd)
    if i == -1:
      print ("Error: not found\n")
      return -1
    inobj = InodeNumber(self.FileObject.RawBlocks,i)
    inobj.InodeNumberToInode()
    if inobj.inode.type != INODE_TYPE_FILE:
      print ("Error: not a file\n")
      return -1
    data, errorcode = self.FileObject.Read(i, 0, MAX_FILE_SIZE)
    if data == -1:
      print ("Error: " + errorcode)
      return -1
    print (data.decode())
    return 0

  # implements showblock (log block n contents)
  def showblock(self, n):

    try:
      n = int(n)
    except ValueError:
      print('Error: ' + n + ' not a valid Integer')
      return -1

    if n < 0 or n >= TOTAL_NUM_BLOCKS:
      print('Error: block number ' + str(n) + ' not in valid range [0, ' + str(TOTAL_NUM_BLOCKS - 1) + ']')
      return -1
    logging.info('Block (strings in block) [' + str(n) + '] : \n' + str((self.FileObject.RawBlocks.Get(n).decode(encoding='UTF-8',errors='ignore'))))
    logging.info('Block (raw hex block) [' + str(n) + '] : \n' + str((self.FileObject.RawBlocks.Get(n).hex())))
    return 0

    # implements showparityblock (displays the parity block for block n)
  def showparityblock(self, n):

    try:
      n = int(n)
    except ValueError:
      print('Error: ' + n + ' not a valid Integer')
      return -1
    if n < 0 or n >= TOTAL_NUM_BLOCKS: #  / (self.FileObject.RawBlocks.NS - 1)
      print('Error: block number ' + str(n) + ' not in valid range [0, ' + str(TOTAL_NUM_BLOCKS - 1) + ']')
      return -1
    parity_server_id = self.FileObject.RawBlocks.getParityServer(n)
    serverd_id, block_number = self.FileObject.RawBlocks.getBlockInfo(n)
    logging.info('Parity server: ' + str(parity_server_id) + ' Row: ' + str(block_number))
    logging.info('Block (strings in block) [' + str(block_number) + '] : \n' + str(
      (self.FileObject.RawBlocks.SingleGet(parity_server_id, block_number).decode(encoding='UTF-8', errors='ignore'))))
    logging.info('Block (raw hex block) [' + str(block_number) + '] : \n' + str(
      (self.FileObject.RawBlocks.SingleGet(parity_server_id, block_number).hex())))
    return 0

# implements showblockslice (log slice of block n contents)
  def showblockslice(self, n, start, end):

    try:
      n = int(n)
    except ValueError:
      print('Error: ' + n + ' not a valid Integer')
      return -1
    try:
      start = int(start)
    except ValueError:
      print('Error: ' + start + ' not a valid Integer')
      return -1
    try:
      end = int(end)
    except ValueError:
      print('Error: ' + end + ' not a valid Integer')
      return -1

    if n < 0 or n >= TOTAL_NUM_BLOCKS:
      print('Error: block number ' + str(n) + ' not in valid range [0, ' + str(TOTAL_NUM_BLOCKS - 1) + ']')
      return -1
    if start < 0 or start >= BLOCK_SIZE:
      print('Error: start ' + str(start) + 'not in valid range [0, ' + str(BLOCK_SIZE-1) + ']')
      return -1
    if end < 0 or end >= BLOCK_SIZE or end <= start:
      print('Error: end ' + str(end) + 'not in valid range [0, ' + str(BLOCK_SIZE-1) + ']')
      return -1

    wholeblock = self.FileObject.RawBlocks.Get(n)
#    logging.info('Block (strings in block) [' + str(n) + '] : \n' + str((wholeblock[start:end+1].decode(encoding='UTF-8',errors='ignore'))))
    logging.info('Block (raw hex block) [' + str(n) + '] : \n' + str((wholeblock[start:end+1].hex())))
    return 0


  # implements showinode (log inode i contents)
  def showinode(self, i):

    try:
      i = int(i)
    except ValueError:
      print('Error: ' + i + ' not a valid Integer')
      return -1

    if i < 0 or i >= MAX_NUM_INODES:
      print('Error: inode number ' + str(i) + ' not in valid range [0, ' + str(MAX_NUM_INODES - 1) + ']')
      return -1

    inobj = InodeNumber(self.FileObject.RawBlocks, i)
    inobj.InodeNumberToInode()
    inode = inobj.inode
    inode.Print()
    return 0

  # implements showfsconfig (log fs config contents)
  def showfsconfig(self):
    self.FileObject.RawBlocks.PrintFSInfo()
    return 0

  # implements repair
  def repair(self, server_id):

    try:
      server_id = int(server_id)
    except ValueError:
      print('Error: ' + server_id + ' not a valid Integer')
      return -1

    self.FileObject.RawBlocks.Repair(server_id)
    return 0

  # implements load (load the specified dump file)
  def load(self, dumpfilename):
    if not os.path.isfile(dumpfilename):
      print("Error: Please provide valid file")
      return -1
    self.FileObject.RawBlocks.LoadFromDisk(dumpfilename)
    self.cwd = 0
    return 0

  # implements save (save the file system contents to specified dump file)
  def save(self, dumpfilename):
    self.FileObject.RawBlocks.DumpToDisk(dumpfilename)
    return 0


  # implements mkdir
  def mkdir(self, dir):
    i, errorcode = self.FileObject.Create(self.cwd, dir, INODE_TYPE_DIR)
    if i == -1:
      print ("Error: " + errorcode + "\n")
      return -1
    return 0

  # implements create
  def create(self, file):
    i, errorcode = self.FileObject.Create(self.cwd, file, INODE_TYPE_FILE)
    if i == -1:
      print ("Error: " + errorcode + "\n")
      return -1
    return 0

  # implements rm
  def rm(self, file):
    i, errorcode = self.FileObject.Unlink(self.cwd, file)
    if i == -1:
      print ("Error: " + errorcode + "\n")
      return -1
    return 0

  # implements append
  def append(self, filename, string):
    # i = self.FileObject.Lookup(filename, self.cwd)
    # Now resolve full path
    i = self.FileObject.GeneralPathToInodeNumber(filename,self.cwd)
    if i == -1:
      print ("Error: not found\n")
      return -1
    inobj = InodeNumber(self.FileObject.RawBlocks,i)
    inobj.InodeNumberToInode()
    if inobj.inode.type != INODE_TYPE_FILE:
      print ("Error: not a file\n")
      return -1
    written, errorcode = self.FileObject.Write(i, inobj.inode.size, bytearray(string,"utf-8"))
    if written == -1:
      print ("Error: " + errorcode)
      return -1
    print ("Successfully appended " + str(written) + " bytes.")
    return 0

  # implements hard link
  def lnh(self, target, name):
    i, errorcode = self.FileObject.Link(target, name, self.cwd)
    if i == -1:
      print ("Error: " + errorcode)
      return -1
    return 0
  # implements soft link
  def lns(self, target, name):
    i, errorcode = self.FileObject.Symlink(target, name, self.cwd)
    if i == -1:
      print ("Error: " + errorcode)
      return -1
    return 0

# test programs
  def test1(self):
    for x in range(4):
      self.create("file" + str(x))
      self.append("file" + str(x),"helloworldLoremIpsumissimplydummytextoftheprintingandtypesettingindustry.LoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumi")
      self.cat("file" + str(x))
    return 0

  def test2(self):
    for x in range(20):
      self.create("file" + str(x))
      self.append("file" + str(x),
                  "helloworldLoremIpsumissimplydummytextoftheprintingandtypesettingindustry.LoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumihelloworldLoremIpsumissimplydummytextoftheprintingandtypesettingindustry.LoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumihelloworldLoremIpsumissimplydummytextoftheprintingandtypesettingindustry.LoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumihelloworldLoremIpsumissimplydummytextoftheprintingandtypesettingindustry.LoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumihelloworldLoremIpsumissimplydummytextoftheprintingandtypesettingindustry.LoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumiLoremIpsumi")
      self.cat("file" + str(x))
    return 0

  def test2_read(self):
    for x in range(20):
      self.cat("file" + str(x))
    return 0

  def test3(selfs):
    for x in range(TOTAL_NUM_BLOCKS):
      selfs.showblock(x)
    return 0

  def Interpreter(self):
    while (True):
      command = input("[cwd=" + str(self.cwd) + "]%")
      splitcmd = command.split()
      if len(splitcmd) == 0:
        continue
      elif splitcmd[0] == "cd":
        if len(splitcmd) != 2:
          print ("Error: cd requires one argument")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.cd(splitcmd[1])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "cat":
        if len(splitcmd) != 2:
          print ("Error: cat requires one argument")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.cat(splitcmd[1])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "ls":
        self.FileObject.RawBlocks.Acquire()
        self.ls()
        self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "showblock":
        if len(splitcmd) != 2:
          print ("Error: showblock requires one argument")
        else:
          self.showblock(splitcmd[1])
      elif splitcmd[0] == "showparityblock":
        if len(splitcmd) != 2:
          print ("Error: showparityblock requires one argument")
        else:
          self.showparityblock(splitcmd[1])
      elif splitcmd[0] == "test1":
        if len(splitcmd) != 1:
          print ("Error: test1 does not require argument")
        else:
          self.test1()
      elif splitcmd[0] == "test2":
        if len(splitcmd) != 1:
          print ("Error: test2 does not require argument")
        else:
          self.test2()
      elif splitcmd[0] == "test2_read":
        if len(splitcmd) != 1:
          print ("Error: test2_read does not require argument")
        else:
          self.test2_read()
      elif splitcmd[0] == "test3":
        if len(splitcmd) != 1:
          print ("Error: test3 do not require argument")
        else:
          self.test3()
      elif splitcmd[0] == "repair":
        if len(splitcmd) != 2:
          print ("Error: repair requires one argument")
        else:
          self.repair(splitcmd[1])
      elif splitcmd[0] == "showblockslice":
        if len(splitcmd) != 4:
          print ("Error: showblockslice requires three arguments")
        else:
          self.showblockslice(splitcmd[1],splitcmd[2],splitcmd[3])
      elif splitcmd[0] == "showinode":
        if len(splitcmd) != 2:
          print ("Error: showinode requires one argument")
        else:
          self.showinode(splitcmd[1])
      elif splitcmd[0] == "showfsconfig":
        if len(splitcmd) != 1:
          print ("Error: showfsconfig do not require argument")
        else:
          self.showfsconfig()
      elif splitcmd[0] == "load":
        if len(splitcmd) != 2:
          print ("Error: load requires 1 argument")
        else:
          self.load(splitcmd[1])
      elif splitcmd[0] == "save":
        if len(splitcmd) != 2:
          print ("Error: save requires 1 argument")
        else:
          self.save(splitcmd[1])
      elif splitcmd[0] == "mkdir":
        if len(splitcmd) != 2:
          print ("Error: mkdir requires one argument")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.mkdir(splitcmd[1])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "create":
        if len(splitcmd) != 2:
          print ("Error: create requires one argument")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.create(splitcmd[1])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "rm":
        if len(splitcmd) != 2:
          print ("Error: rm requires one argument")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.rm(splitcmd[1])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "append":
        if len(splitcmd) != 3:
          print ("Error: append requires two arguments")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.append(splitcmd[1],splitcmd[2])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "lnh":
        if len(splitcmd) != 3:
          print ("Error: lnh requires two arguments")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.lnh(splitcmd[1], splitcmd[2])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "lns":
        if len(splitcmd) != 3:
          print ("Error: lns requires two arguments")
        else:
          self.FileObject.RawBlocks.Acquire()
          self.lns(splitcmd[1], splitcmd[2])
          self.FileObject.RawBlocks.Release()
      elif splitcmd[0] == "exit":
        return
      else:
        print ("command " + splitcmd[0] + " not valid.\n")


if __name__ == "__main__":

  # Initialize file for logging
  # Change logging level to INFO to remove debugging messages
  logging.basicConfig(filename='memoryfs.log', filemode='w', level=logging.DEBUG)


  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-ni', '--max_num_inodes', type=int, help='an integer value')
  ap.add_argument('-is', '--inode_size', type=int, help='an integer value')
  # add port and client id
  ap.add_argument('-ns', '-ns', type=int, help='an integer value')
  ap.add_argument('-startport', '--startport', type=int, help='an integer value')
  ap.add_argument('-cid', '--cid', type=int, help='an integer value')
  # set to zero to skip initializing root inode
  ap.add_argument('-rootinit', '--rootinit', default=1, type=int, help='an integer value')

  # Other than FS args, consecutive args will be captured in by 'arg' as list
  ap.add_argument('arg', nargs='*')

  args = ap.parse_args()

  # Initialize empty file system data
  logging.info('Initializing data structures...')
  RawBlocks = DiskBlocks(args)
  # no need to initialize blocks; they are initialized server-side 
  # boot_block = b'\x12\x34\x56\x78' # constant 12345678 stored as beginning of boot block; no need to change this
  # RawBlocks.InitializeBlocks(boot_block)


  # Print file system information and contents of first few blocks to memoryfs.log
  RawBlocks.PrintFSInfo()
  RawBlocks.PrintBlocks("Initialized",0,16)

  # Initialize FileObject inode
  FileObject = FileName(RawBlocks)

  # reload the global variables (in case they changed due to command line inputs)
  from memoryfs_client import *

  # Initalize root inode
  if args.rootinit == 1:
    FileObject.InitRootInode()

  # Redirect INFO logs to console as well
  console_handler = logging.StreamHandler()
  console_handler.setLevel(logging.INFO)
  logging.getLogger().addHandler(console_handler)

  # Run the interactive shell interpreter
  myshell = FSShell(FileObject)
  myshell.Interpreter()

