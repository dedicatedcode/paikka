package com.dedicatedcode.paikka.flatbuffers;
import java.nio.*;
import java.lang.*;

import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Boundary extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static Boundary getRootAsBoundary(ByteBuffer _bb) { return getRootAsBoundary(_bb, new Boundary()); }
  public static Boundary getRootAsBoundary(ByteBuffer _bb, Boundary obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Boundary __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long osmId() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public int level() { int o = __offset(6); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  public String name() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ByteBuffer nameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 8, 1); }
  public Geometry geometry() { return geometry(new Geometry()); }
  public Geometry geometry(Geometry obj) { int o = __offset(10); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }

  public static int createBoundary(FlatBufferBuilder builder,
      long osmId,
      int level,
      int nameOffset,
      int geometryOffset) {
    builder.startTable(4);
    Boundary.addOsmId(builder, osmId);
    Boundary.addGeometry(builder, geometryOffset);
    Boundary.addName(builder, nameOffset);
    Boundary.addLevel(builder, level);
    return Boundary.endBoundary(builder);
  }

  public static void startBoundary(FlatBufferBuilder builder) { builder.startTable(4); }
  public static void addOsmId(FlatBufferBuilder builder, long osmId) { builder.addLong(0, osmId, 0L); }
  public static void addLevel(FlatBufferBuilder builder, int level) { builder.addInt(1, level, 0); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(2, nameOffset, 0); }
  public static void addGeometry(FlatBufferBuilder builder, int geometryOffset) { builder.addOffset(3, geometryOffset, 0); }
  public static int endBoundary(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishBoundaryBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedBoundaryBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Boundary get(int j) { return get(new Boundary(), j); }
    public Boundary get(Boundary obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

