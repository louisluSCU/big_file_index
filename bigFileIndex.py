import io
import os
import hashlib


INPUT_FILE = 'tmp.log'
INDEX_FILE = 'index.log'
SLICE_SIZE = 2**30
KEY_SIZE_S = 2
VALUE_SIZE_S = 2
HASH_SIZE = 4
CPU_ENDIAN = 'big'


class IndexBuilder:

    def __init__(self):
        self.table_size = 2**(HASH_SIZE*8)*6
        self.tail = 2**(HASH_SIZE*8)*6

    # Read 2GB data at a time
    def __read_file_slice(self):
        fp = open(INPUT_FILE, 'rb')
        while True:
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
            self.__write_index(hash_v*6, [item[1], ], fp)
        fp.close()

    def __write_index(self, offset, data, fp):
        fp.seek(offset)
        tuple = fp.read(6)

        # No conflict
        if tuple == b'':
            if offset == self.tail:
                self.tail += len(data)*6
            for item in data:
                # print('write {} at {}'.format(str(str(item)), fp.tell()))
                fp.write((0).to_bytes(1, byteorder=CPU_ENDIAN) + item)
        elif tuple == b'\x00\x00\x00\x00\x00\x00':
            fp.seek(-6, 1)
            for item in data:
                # print('write {} at {}'.format(str(str(item)), fp.tell()))
                fp.write((0).to_bytes(1, byteorder=CPU_ENDIAN) + item)

        # Hash conflict
        else:
            fp.seek(-6, 1)
            number_dup = int.from_bytes(fp.read(1), byteorder=CPU_ENDIAN)

            if number_dup == 254:
                raise OverflowError

            fp.seek(-1, 1)
            fp.write((number_dup + 1).to_bytes(1, byteorder=CPU_ENDIAN))

            # Data tuple
            if number_dup == 0:
                data.append(fp.read(5))
                fp.seek(-5, 1)
                fp.write(self.tail.to_bytes(5, byteorder=CPU_ENDIAN))

            # Index tuple
            else:
                next_node = fp.read(5)
                fp.seek(-5, 1)
                fp.write(self.tail.to_bytes(5, byteorder=CPU_ENDIAN))
                fp.seek(int.from_bytes(next_node, byteorder=CPU_ENDIAN))

                for i in range(number_dup+1):
                    fp.write((255).to_bytes(1, byteorder=CPU_ENDIAN))
                    data.append(fp.read(5))

            self.__write_index(self.tail, data, fp)

    def build(self):
        counter = 0
        for file_slice in self.__read_file_slice():
            kv_pairs = []
            fp = io.BytesIO(file_slice)

            while True:
                # Calculate offset in file
                index = (SLICE_SIZE * counter + fp.tell()).to_bytes(5, byteorder=CPU_ENDIAN)
                key_size = fp.read(KEY_SIZE_S)

                # End of this slice
                if not key_size:
                    break

                key = fp.read(int.from_bytes(key_size, byteorder=CPU_ENDIAN))
                kv_pairs.append((key, index))
                value_size = fp.read(VALUE_SIZE_S)
                fp.read(int.from_bytes(value_size, byteorder=CPU_ENDIAN))

            fp.close()
            print(kv_pairs)
            self.__add_index(kv_pairs)
            counter += 1

        # Index file compaction
        self.index_compaction()

    def index_compaction(self):
        # Temp file for middle result
        middle = open('middle.log', 'wb+')
        index = open(INDEX_FILE, 'rb+')

        # Update linked list
        counter = 0
        index.seek(self.table_size)
        while index.tell() < self.tail:
            flag = index.read(1)

            # Garbage tuple
            if int.from_bytes(flag, byteorder=CPU_ENDIAN) == 255:
                counter += 1
                index.read(5)

            # Data tuple
            else:
                index.seek(-1, 1)

                middle.seek(index.tell()-self.table_size)
                middle.write(counter.to_bytes(5, byteorder=CPU_ENDIAN))
                # print('write offset {} at {} in middle file'.format(str(counter), str(index.tell()-self.table_size)))

                data = index.read(6)
                index.seek(-(counter+1)*6, 1)
                index.write((0).to_bytes(1, byteorder=CPU_ENDIAN)+data[1:])
                index.seek(counter*6, 1)

        # Update hash table
        index.seek(0)
        while index.tell() < self.table_size:
            flag = index.read(1)

            # Conflict
            if int.from_bytes(flag, byteorder=CPU_ENDIAN) != 0:
                old_pos = int.from_bytes(index.read(5), byteorder=CPU_ENDIAN)
                middle.seek(old_pos-self.table_size)
                offset = int.from_bytes(middle.read(5), byteorder=CPU_ENDIAN)
                new_pos = old_pos - offset*6
                index.seek(-5, 1)
                index.write(new_pos.to_bytes(5, byteorder=CPU_ENDIAN))
            else:
                index.read(5)

        # delete middle
        middle.close()
        os.remove('middle.log')
        index.close()

        self.tail -= counter*6
        print('{} bytes of space saved, tail now at {}'.format(str(counter*6), str(self.tail)))



class DataReader:
    def __init__(self):
        pass

    def __search_index(self, offset, fp, dup=0):
        fp.seek(offset)
        tuple = fp.read(6)

        # Key not exist
        if tuple in (b'\x00\x00\x00\x00\x00\x00', b''):
            return []

        else:
            number_dup = int.from_bytes(tuple[0:1], byteorder=CPU_ENDIAN)
            values = []

            # No conflict
            if number_dup == 0:
                address = int.from_bytes(tuple[1:], byteorder=CPU_ENDIAN)
                values.append(address)
                while dup > 0:
                    next_tuple = fp.read(6)
                    values.append(int.from_bytes(next_tuple[1:], byteorder=CPU_ENDIAN))
                    dup -= 1

            # Hash conflict
            else:
                if dup == 0:
                    dup = number_dup
                address = int.from_bytes(tuple[1:], byteorder=CPU_ENDIAN)
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
                key_candidate = fp.read(int.from_bytes(key_size, byteorder=CPU_ENDIAN))
                if key_candidate == key:
                    value_size = int.from_bytes(fp.read(VALUE_SIZE_S), byteorder=CPU_ENDIAN)
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
        disk_pos = self.__search_index(hash_v*6, fp)
        print(disk_pos)

        # Find value
        value = self.__search_disk(key, disk_pos)

        fp.close()
        return value


if __name__ == "__main__":
    fib = IndexBuilder()
    fib.build()
    fr = DataReader()
    print(fr.get('LUYE'))
