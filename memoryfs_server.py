import pickle, logging
import argparse
import time
import dbm
import os.path
import hashlib

# For locks: RSM_UNLOCKED=0 , RSM_LOCKED=1 
RSM_UNLOCKED = bytearray(b'\x00') * 1
RSM_LOCKED = bytearray(b'\x01') * 1

PUT_REQUESTS = 0
GET_REQUESTS = 0

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)

class DiskBlocks():
  def __init__(self, total_num_blocks, block_size):
    # This class stores the raw block array
    self.block = []
    self.checksum = {}
    # Initialize raw blocks
    for i in range (0, total_num_blocks):
      putdata = bytearray(block_size)
      hash = hashlib.md5(putdata)
      self.block.insert(i, putdata)
      self.checksum[i] = hash.digest()

if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-cblk', '--cblk', type=int, help='an integer value')

  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks') 
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT = args.port
  else:
    print('Must specify port number')
    quit()

  if args.cblk:
    CBLK = args.cblk
  else:
    CBLK = -1


  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE)

  # Create server
  server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)

  def Get(block_number):
    global GET_REQUESTS
    GET_REQUESTS += 1
    print("GETS: " + str(GET_REQUESTS) + " PUTS: " + str(PUT_REQUESTS) + " TOTAL: " + str(GET_REQUESTS + PUT_REQUESTS))
    result = RawBlocks.block[block_number]
    hash = hashlib.md5(result)
    #print(hash.digest())
    #print(RawBlocks.checksum[block_number])
    if hash.digest() != RawBlocks.checksum[block_number] or block_number == CBLK:
      result = -1
    return result

  server.register_function(Get)

  def Put(block_number, data):
    global PUT_REQUESTS
    PUT_REQUESTS += 1
    print("GETS: " + str(GET_REQUESTS) + " PUTS: " + str(PUT_REQUESTS) + " TOTAL: " + str(GET_REQUESTS + PUT_REQUESTS))
    RawBlocks.block[block_number] = data.data
    hash = hashlib.md5(data.data)
    RawBlocks.checksum[block_number] = hash.digest()
    return 0

  server.register_function(Put)

  def RSM(block_number):
    result = RawBlocks.block[block_number]
    hash = hashlib.md5(result)
    if hash.digest() != RawBlocks.checksum[block_number]:
      return -1
    # RawBlocks.block[block_number] = RSM_LOCKED
    RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
    hash = hashlib.md5(bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01')))
    RawBlocks.checksum[block_number] = hash.digest()
    return result

  server.register_function(RSM)

  # Run the server's main loop
  print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + ", cblk=" + str(CBLK) + " on port " + str(PORT))
  server.serve_forever()

