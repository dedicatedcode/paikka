/*
 *  This file is part of paikka.
 *
 *  Paikka is free software: you can redistribute it and/or
 *  modify it under the terms of the GNU Affero General Public License
 *  as published by the Free Software Foundation, either version 3 or
 *  any later version.
 *
 *  Paikka is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied
 *  warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with Paikka. If not, see <https://www.gnu.org/licenses/>.
 */

package com.dedicatedcode.paikka.service;

import com.dedicatedcode.paikka.IntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.junit.jupiter.api.Assertions.*;

@IntegrationTest
class S2HelperTest {
    
    private final S2Helper s2Helper = new S2Helper();
    
    @Test
    void testGetShardId() {
        // Test with Berlin coordinates
        double lat = 52.5200;
        double lon = 13.4050;
        
        long shardId = s2Helper.getShardId(lat, lon);
        
        // Should return a valid S2 cell ID
        assertNotEquals(0, shardId);
        
        long shardId2 = s2Helper.getShardId(lat, lon);
        assertEquals(shardId, shardId2);
    }
    
    @Test
    void testGetShardIdConsistency() {
        // Test that nearby coordinates might share the same shard
        double lat1 = 52.5200;
        double lon1 = 13.4050;
        double lat2 = 52.5201; // Very close
        double lon2 = 13.4051;
        
        long shardId1 = s2Helper.getShardId(lat1, lon1);
        long shardId2 = s2Helper.getShardId(lat2, lon2);
        
        // Both should be valid
        assertNotEquals(0, shardId1);
        assertNotEquals(0, shardId2);
    }
    
    @Test
    void testLongToByteArrayConversion() {
        long testValue = 123456789L;
        
        byte[] bytes = s2Helper.longToByteArray(testValue);
        long converted = s2Helper.byteArrayToLong(bytes);
        
        assertEquals(testValue, converted);
    }
    
    @Test
    void testLongToByteArrayWithNegativeValue() {
        long testValue = -123456789L;
        
        byte[] bytes = s2Helper.longToByteArray(testValue);
        long converted = s2Helper.byteArrayToLong(bytes);
        
        assertEquals(testValue, converted);
    }
    
    @Test
    void testLongToByteArrayWithZero() {
        long testValue = 0L;
        
        byte[] bytes = s2Helper.longToByteArray(testValue);
        long converted = s2Helper.byteArrayToLong(bytes);
        
        assertEquals(testValue, converted);
    }
    
    @Test
    void testLongToByteArrayWithMaxValue() {
        long testValue = Long.MAX_VALUE;
        
        byte[] bytes = s2Helper.longToByteArray(testValue);
        long converted = s2Helper.byteArrayToLong(bytes);
        
        assertEquals(testValue, converted);
    }

}
