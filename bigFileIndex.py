import io
import hashlib


INPUT_FILE = 'tmp.log'
INDEX_FILE = 'index.log'
SLICE_SIZE = 2**30
KEY_SIZE_S = 2
VALUE_SIZE_S = 2
# TODO: HASH_SIZE = 4
HASH_SIZE = 1


class IndexBuilder:

    def __init__(self):
        self.tail = 2**(HASH_SIZE*8)*6

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
            self.__write_index(hash_v, [item[1],], fp)
        fp.close()

    def __write_index(self, offset, data, fp):
        fp.seek(offset*6)
        tuple = fp.read(6)

        if tuple == b'':
            if offset == self.tail:
                self.tail += len(data)*6
            for item in data:
                fp.write((0).to_bytes(1, byteorder='big') + item)
        elif tuple == b'\x00\x00\x00\x00\x00\x00':
            fp.seek(-6, 1)
            for item in data:
                fp.write((0).to_bytes(1, byteorder='big') + item)
        else:
            fp.seek(-6, 1)
            number_dup = int.from_bytes(fp.read(1), byteorder='big')

            if number_dup == 255:
                raise OverflowError

            fp.seek(-1, 1)
            fp.write((number_dup + 1).to_bytes(1, byteorder='big'))
            next_node = fp.read(5)

            # Data tuple
            if number_dup == 0:
                fp.seek(-5, 1)
                fp.write(self.tail.to_bytes(5, byteorder='big'))
                data.append(next_node)
                self.__write_index(self.tail, data, fp)

            # Index tuple
            else:
                self.__write_index(int.from_bytes(next_node, byteorder='big'), data, fp)

    def build(self):
        counter = 0
        for file_slice in self.__read_file_slice():
            kv_pairs = []
            fp = io.BytesIO(file_slice)

            while True:
                # Calculate offset in file
                index = (SLICE_SIZE * counter + fp.tell()).to_bytes(5, byteorder='big')
                key_size = fp.read(KEY_SIZE_S)

                # End of this slice
                if not key_size:
                    break

                # TODO: key = fp.read(int.from_bytes(key_size, byteorder='big'))
                key = fp.read(int(key_size))
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

    def __search_index(self, offset, fp, dup=0):
        fp.seek(offset * 6)
        tuple = fp.read(6)

        # Key not exist
        if tuple in (b'\x00\x00\x00\x00\x00\x00', b''):
            return []

        else:
            number_dup = int.from_bytes(tuple[0:1], byteorder='big')
            values = []

            # No duplicate
            if number_dup == 0:
                address = int.from_bytes(tuple[1:], byteorder='big')
                values.append(address)
                while dup != 0:
                    next_tuple = fp.read(6)
                    values.append(int.from_bytes(next_tuple[1:], byteorder='big'))
                    dup -= 1

            else:
                if dup == 0:
                    dup = number_dup
                address = int.from_bytes(tuple[1:], byteorder='big')
                values = self.__search_index(address, fp, dup)

            return values

    def __search_disk(self, key, offsets):
        value = None

        if len(offsets) == 0:
            return value

        else:
            fp = open(INPUT_FILE, 'rb')
            # In case insertions of tuples with the same key, always return most recent one
            offsets.sort(reverse=True)

            # Check all possible keys
            for offset in offsets:
                fp.seek(offset)
                key_size = fp.read(KEY_SIZE_S)
                # TODO: key_candidate = fp.read(int.from_bytes(key_size, byteorder='big'))
                key_candidate = fp.read(int(key_size))
                if key_candidate == key:
                    # TODO: value_size = int.from_bytes(fp.read(VALUE_SIZE_S), byteorder='big')
                    value_size = int(fp.read(VALUE_SIZE_S))
                    value = fp.read(value_size)
                    break

            fp.close()
            return value

    def get(self, key):
        if type(key) is not 'bytes':
            try:
                key = key.encode('ascii')
            except:
                print("Unsupported key type")
                return None

        h = hashlib.blake2b(digest_size=HASH_SIZE)
        h.update(key)
        hash_v = int(h.hexdigest(), 16)

        # Search index
        fp = open(INDEX_FILE, 'rb')
        disk_pos = self.__search_index(hash_v, fp)

        # Find value
        value = self.__search_disk(key, disk_pos)

        fp.close()
        return value





if __name__ == "__main__":
    fib = IndexBuilder()
    fib.build()
    fr = DataReader()
    print(fr.get('CC'))
