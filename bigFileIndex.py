import io
import hashlib


INPUT_FILE = 'tmp.log'
INDEX_FILE = 'index.log'
SLICE_SIZE = 2**30
KEY_SIZE_S = 2
VALUE_SIZE_S = 2
HASH_SIZE = 1


class IndexBuilder:

    def __init__(self):
        pass

    # Read 2GB data at a time
    def __read_file_slice(self):
        fp = open(INPUT_FILE, 'rb')
        while True:
            # TODO: read cross slice tuple
            slice_data = fp.read(SLICE_SIZE)
            if not slice_data:
                break
            yield slice_data
        fp.close()

    def __add_index(self, kv_pairs):
        fp = open(INDEX_FILE, 'wb+')
        for item in kv_pairs:
            h = hashlib.blake2b(digest_size=HASH_SIZE)
            h.update(item[0])
            hash_v = int(h.hexdigest(), 16)
            print(hash_v)
            # TODO: hash conflict
            fp.seek(hash_v*5)
            fp.write(item[1])
        fp.close()

    def build(self):
        counter = 0
        for file_slice in self.__read_file_slice():
            kv_pairs = []
            fp = io.BytesIO(file_slice)

            while True:
                key_size = fp.read(KEY_SIZE_S)
                if not key_size:
                    break
                # TODO: key = fp.read(int.from_bytes(key_size, byteorder='big'))
                key = fp.read(int(key_size))

                # Calculate offset in file
                index = (SLICE_SIZE * counter + fp.tell()).to_bytes(5, byteorder='big')
                print(index)
                kv_pairs.append((key, index))
                value_size = fp.read(VALUE_SIZE_S)
                # TODO: fp.read(int.from_bytes(value_size, byteorder='big'))
                fp.read(int(value_size))

            fp.close()
            print(kv_pairs)
            self.__add_index(kv_pairs)
            counter += 1


class DataReader:
    def __init__(self):
        pass

    def get(self, key):
        if type(key) is not 'bytes':
            try:
                key = key.encode('ascii')
            except:
                print("Unsupported key type")

        h = hashlib.blake2b(digest_size=HASH_SIZE)
        h.update(key)
        hash_v = int(h.hexdigest(), 16)

        # Search index
        fp = open(INDEX_FILE, 'rb')
        fp.seek(hash_v*5)
        offset = fp.read(5)

        # Key not exist
        if offset == b'\x00\x00\x00\x00\x00':
            return None
        offset = int.from_bytes(offset, byteorder='big')
        fp.close()

        # Search file with offset
        fp = open(INPUT_FILE, 'r')
        fp.seek(offset)
        value_size = int(fp.read(VALUE_SIZE_S))
        value = fp.read(value_size)
        fp.close()

        return value


if __name__ == "__main__":
    # fib = IndexBuilder()
    # fib.build()
    fr = DataReader()
    print(fr.get('AAA'))
