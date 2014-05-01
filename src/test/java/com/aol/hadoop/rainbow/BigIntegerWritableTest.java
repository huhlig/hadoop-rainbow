package com.aol.hadoop.rainbow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class BigIntegerWritableTest {
    
    
    /**
     * Test of get method, of class BigIntegerWritable.
     */
    @Test
    public void testSerde() throws IOException {
        System.out.println("testSerde");
        final BigIntegerWritable original = new BigIntegerWritable(123456789L);
        final BigIntegerWritable result = new BigIntegerWritable();
        
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(baos);
        original.write(dos);
        dos.flush();
        
        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final DataInputStream dis = new DataInputStream(bais);
        result.readFields(dis);
        
        System.out.println(original);
        System.out.println(result);
        
        assertEquals(original, result);
    }    
}
