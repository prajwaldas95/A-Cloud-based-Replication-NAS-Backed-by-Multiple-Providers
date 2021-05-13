from basic_defs import cloud_storage, NAS
import boto3
import hashlib
from azure.storage.blob import BlobServiceClient
from google.cloud import storage

block_size = 4096
class AWS_S3(cloud_storage):
    def __init__(self):
        # TODO: Fill in the AWS access key ID
        self.access_key_id = "AKIAZ3WFZEEF2ARUKYJL"
        # TODO: Fill in the AWS access secret key
        self.access_secret_key = "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"
        # TODO: Fill in the bucket name
        self.bucket_name = "csce678-s21-p1-330007250"
        self.s3 = boto3.resource('s3', aws_access_key_id=self.access_key_id, aws_secret_access_key=self.access_secret_key)
        self.bucket = self.s3.Bucket(self.bucket_name)
        self.client = boto3.client('s3', aws_access_key_id=self.access_key_id, aws_secret_access_key=self.access_secret_key)
    def list_blocks(self):
        keys = list()
        for file in self.bucket.objects.all():
            keys.append(int(file.key))
        return keys
#offset is the hash key
    def read_block(self, offset):
        try:
            offset=str(offset)
            obj = self.s3.Object(bucket_name=self.bucket_name, key=offset)
            response = obj.get()
            return bytearray(response["Body"].read())
        except:
            return bytearray("")

    def write_block(self, block, offset):
        offset = str(offset)
        block = str(block)
        bucket = self.s3.Bucket(self.bucket_name)
        bucket.put_object(Key=offset,Body=block)

    def delete_block(self, offset):
        offset = str(offset)
        self.client.delete_object(Bucket=self.bucket_name, Key=offset)

class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        # TODO: Fill in the Azure key
        self.key = "u04DPr/UGGADYcl27vrXG3lAZ7cMP7LC+4Y3NKuR3nL8jLkp0xwG9NRzfCtDHG2nn4xX4adldrHfFmhRtT3afA=="
        # TODO: Fill in the Azure connection string
        self.conn_str = "DefaultEndpointsProtocol=https;AccountName=csce678s21;AccountKey=u04DPr/UGGADYcl27vrXG3lAZ7cMP7LC+4Y3NKuR3nL8jLkp0xwG9NRzfCtDHG2nn4xX4adldrHfFmhRtT3afA==;EndpointSuffix=core.windows.net"
        # TODO: Fill in the account name
        self.account_name = "csce678s21"
        # TODO: Fill in the container name
        self.container_name = "csce678-s21-p1-330007250"
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)

    def list_blocks(self):
        blob_list = self.blob_service_client.get_container_client(self.container_name).list_blobs()
        keys = list()
        for file in blob_list:
            keys.append(int(file.name))
        return keys


    def read_block(self, offset):
        try:
            offset = str(offset)
            blob_client = self.blob_service_client.get_blob_client(self.container_name, str(offset))
            if blob_client.exists():
                download_stream = blob_client.download_blob()
                return bytearray(download_stream.readall())
            else:
                return bytearray("")
        except:
            return bytearray("")

    def write_block(self, block, offset):
        offset = str(offset)
        block=str(block)
        blob_client = self.blob_service_client.get_blob_client(self.container_name, str(offset))
        if(blob_client.exists()):
            blob_client.delete_blob()
        blob_client.upload_blob(block, blob_type="BlockBlob")


    def delete_block(self, offset):
        offset = str(offset)
        blob_client = self.blob_service_client.get_blob_client(self.container_name, str(offset))
        if blob_client.exists():
            blob_client.delete_blob()
        return 0


class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        # Google Cloud Storage is authenticated with a **Service Account**
        # TODO: Download and place the Credential JSON file
        self.credential_file = "gcp-credential.json"
        # TODO: Fill in the container name
        self.bucket_name = "csce678-s21-p1-330007250"
        self.storage_client = storage.Client.from_service_account_json(self.credential_file)
        self.bucket = self.storage_client.get_bucket(self.bucket_name)


    def list_blocks(self):
        blob_list=self.storage_client.list_blobs(self.bucket_name)
        keys = list()
        for file in blob_list:
            keys.append(int(file.name))
        return keys

    def read_block(self, offset):
        try:
            offset = str(offset)
            blob=self.bucket.get_blob(str(offset))
            return bytearray(blob.download_as_string())
        except:
            return bytearray("")

    def write_block(self, block, offset):
        offset = str(offset)
        block = str(block)
        blob = self.bucket.get_blob(str(offset))
        if blob!=None:
            blob.delete()
        new_blob = self.bucket.blob(str(offset))
        new_blob.upload_from_string(block)


    def delete_block(self, offset):
        offset = str(offset)
        blob = self.bucket.get_blob(str(offset))
        if blob!=None:
            self.bucket.delete_blob(str(offset))
            return 1
        return 0


class RAID_on_Cloud(NAS):

    def __init__(self):
        self.backends = [
            AWS_S3(),
            Google_Cloud_Storage(),
            Azure_Blob_Storage()
        ]
        self.fds=dict()

    def hash_generator(self,s):
        result = hashlib.md5(s)
        key = (int(result.hexdigest(), 16))%100000000
        return int(key)
    def cloud_mapping(self,s):
        result = hashlib.md5(s)
        key = (int(result.hexdigest(), 16))%100000000
        if(key%3==0):
            return [0,1]
        elif(key%3==1):
            return [1,2]
        else:
            return [2,0]
    def open(self, filename):
        realfd = filename
        newfd = None
        for fd in range(256):
            if fd not in self.fds:
                newfd = fd
                break
        if newfd is None:
            raise IOError("Opened files exceed system limitation.")
        self.fds[newfd] = realfd
        return newfd


    def read(self, fd, len, offset):
        if fd not in self.fds:
            return ""
            raise IOError("File descriptor %d does not exist." % fd)
        #realfd is file name
        realfd = self.fds[fd]
        key=self.hash_generator(realfd)
        map_keys = self.cloud_mapping(realfd)
        i = map_keys[0]
        result = ""
        key2=key
        len3=len
        offset2=offset
        block_offset = key + offset/block_size
        if (offset%block_size>0):
            if(offset%block_size+len>block_size):
                block = str(self.backends[i].read_block(str(block_offset)))
                block_offset=block_offset+1
                len = len-(block_size-offset%block_size)
                result = block[offset % block_size:]
                offset = 0
            else:
                block = str(self.backends[i].read_block(str(block_offset)))
               # print(block)
                result = block[offset % block_size:offset % block_size + len]
              #  result= result.strip('\0')
                len=0
                offset=0
        offset=offset%block_size
        #passing hash key to read_block
        block = str(self.backends[i].read_block(str(block_offset)))
        len=len+offset
        len2=len
        while (len/block_size>0):
            len=len-block_size
            block=str(self.backends[i].read_block(str(block_offset)))
            result = result + block
            block_offset = block_offset + 1
        if ( len2%block_size):
            block = str(self.backends[i].read_block(str(block_offset)))
            result=result+ block[:len2%block_size]
        if(result==''):
            len=len3
            offset = offset2
            i = map_keys[1]
            block_offset = key2 + offset / block_size
            if (offset % block_size > 0):
                if (offset % block_size + len > block_size):
                    block = str(self.backends[i].read_block(str(block_offset)))
                    block_offset = block_offset + 1
                    len = len - (block_size - offset % block_size)
                    result = block[offset % block_size:]
                    offset = 0
                else:
                    block = str(self.backends[i].read_block(str(block_offset)))
                    result = str(block[offset % block_size:offset % block_size + len])
                   # result = result.strip('\0')
                    len = 0
                    offset = 0
            offset = offset % block_size

            block = str(self.backends[i].read_block(str(block_offset)))
            len = len + offset
            len2 = len
            while (len / block_size > 0):
                len = len - block_size
                block = str(self.backends[i].read_block(str(block_offset)))
                result = result + block
                block_offset = block_offset + 1

            if (len2 % block_size):
                block = str(self.backends[i].read_block(str(block_offset)))
                result = result + block[:len2 % block_size]

        return result#.strip('\0')

    def write(self, fd, data, offset):
        if fd not in self.fds:
            return
            raise IOError("File descriptor %d does not exist." % fd)

        size = len(data)
        realfd = self.fds[fd]
        num = offset/block_size
        offset = offset % block_size
        map_keys = self.cloud_mapping(realfd)
        read=str(self.backends[map_keys[0]].read_block(str(self.hash_generator(realfd)+num)))
        readcp=read
        if(len(read)<offset):
            read = read+"\0"*(offset-len(read))
        else:
            read=read[0:offset]

        i = num
        if(offset+size<block_size):
            str_temp = ""
            if(len(readcp)>len(read)+len(data)):
                str_temp=readcp[len(read)+len(data):]
            self.backends[map_keys[0]].write_block(read+data+str_temp, str(self.hash_generator(realfd) + i))
            self.backends[map_keys[1]].write_block(read+ data+str_temp, str(self.hash_generator(realfd) + i))
            return
        if(offset+size>block_size):
            self.backends[map_keys[0]].write_block(read + data[0:block_size-offset],str(self.hash_generator(realfd) + i))
            self.backends[map_keys[1]].write_block(read + data[0:block_size-offset],str(self.hash_generator(realfd) + i))
            size = size+offset-block_size
            data = data[block_size-offset:]
            i = i+1
        while(size>block_size):
            self.backends[map_keys[0]].write_block(data[0:block_size], str(self.hash_generator(realfd)+i))
            self.backends[map_keys[1]].write_block(data[0:block_size], str(self.hash_generator(realfd)+i))
            i = i+1
            size=size-block_size
            data=data[block_size:]
        if(size>0):
            read_last = str(self.backends[map_keys[0]].read_block(str(self.hash_generator(realfd) + i)))
            data_last=data[0:size]
            if(len(read_last)>size):
                data_last=data_last+read_last[size:]
            self.backends[map_keys[0]].write_block(data_last, str(self.hash_generator(realfd)+i))
            self.backends[map_keys[1]].write_block(data_last, str(self.hash_generator(realfd)+i))

    def close(self, fd):
        if fd not in self.fds:
            raise IOError("File descriptor %d does not exist." % fd)

        del self.fds[fd]
        return

    def delete(self, filename):
        key = self.hash_generator(filename)
        key2=key
        map_keys = self.cloud_mapping(filename)
        if(map_keys[0]==0 or map_keys[1]==0):
            client = self.backends[0].client
            try:
                while(True):
                    client.head_object(Bucket=self.backends[0].bucket_name, Key=str(key2))
                    self.backends[0].delete_block(str(key2))
                    key2=key2+1
            except:
                pass
            temp=0
            if(map_keys[1]==1 or map_keys[0]==1):
                temp =1
            else:
                temp=2
            while(self.backends[temp].delete_block(str(key))==1):
                key=key+1
        else:
            try:
                key2 =key
                while(self.backends[1].delete_block(str(key))==1):
                    self.backends[2].delete_block(str(key))
                    key=key+1
                key=key2
                while (self.backends[2].delete_block(str(key)) == 1):
                    key = key + 1
            except:
                return

    def get_storage_sizes(self):
        return [len(b.list_blocks()) for b in self.backends]
