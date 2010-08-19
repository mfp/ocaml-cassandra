(*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied. See the License for the
 specific language governing permissions and limitations
 under the License.
*)

open Thrift
module T = Transport

class t (tr:Transport.t) =
object (self)
  val mutable frame = 0
  val ibyte = String.create 8
  val writeBuf = Buffer.create 1024 (* FIXME String length limitation *)
  inherit Transport.t
  method isOpen = tr#isOpen
  method opn = tr#opn
  method close = tr#close
  method read buf off len =
    if frame >= len then
      let got = tr#read buf off len in
      frame <- frame - got;
      got
    else
    if frame = 0 then
    begin
      self#readFrame;
      self#read buf off len
    end
    else
      raise (T.E (T.UNKNOWN, Printf.sprintf "TFramedTransport: Cross-frame read of size %d (left %d)" len frame))

  method private readFrame =
    assert (frame = 0);
    ignore (tr#readAll ibyte 0 4);
    frame <- TBinaryProtocol.comp_int ibyte 4

  method write buf off len = Buffer.add_substring writeBuf buf off len

  method flush =
    let buf = Buffer.contents writeBuf in
    let len = String.length buf in
    for i=0 to 3 do
      ibyte.[3-i] <- char_of_int (TBinaryProtocol.get_byte len i)
    done;
    Buffer.reset writeBuf;
    tr#write ibyte 0 4;
    tr#write buf 0 len;
    tr#flush
end

