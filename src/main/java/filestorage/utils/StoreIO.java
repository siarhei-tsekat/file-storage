package filestorage.utils;

import filestorage.internals.core.Page;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class StoreIO {
    //todo:tsekot manage concurrent access to one file

    public static void writeToFile(String path, Stream<? extends Page> dataStream) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw")) {
            try (FileChannel fileChannel = randomAccessFile.getChannel()) {

                dataStream.forEach(Try.rethrowing((data) -> fileChannel.write(ByteBuffer.wrap(data.toBytes()), data.filePosition())));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeToFile(String path, byte[] bytes, long position) {

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path, "rw")) {
            try (FileChannel fileChannel = randomAccessFile.getChannel()) {

                fileChannel.write(ByteBuffer.wrap(bytes), position);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] readFromFile(Path path, long position, int length) {

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(path.toString(), "r")) {
            try (FileChannel fileChannel = randomAccessFile.getChannel()) {

                fileChannel.position(position);
                ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                fileChannel.read(byteBuffer);

                byte[] array = byteBuffer.array();

                byteBuffer.clear();

                return array;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static long getFileLength(Path filePath) {
        try {
            return Files.size(filePath);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

interface Try<T> {

    void run(T t) throws Exception;

    static <T> Consumer<T> rethrowing(Try<T> function) {
        return (t) -> {
            try {
                function.run(t);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}

