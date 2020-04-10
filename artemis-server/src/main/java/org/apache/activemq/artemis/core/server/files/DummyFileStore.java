package org.apache.activemq.artemis.core.server.files;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

/**
 * Dummy implementation of FileStore object. Its a workaround to handle the exception thrown from org.apache.activemq.artemis.core.server.files.FileStoreMonitor.addStore(File)
 * when running on a RAMDISK machine or similar file system
 *
 */
class DummyFileStore extends FileStore {

   @Override
   public String name() {
      return "FS_UNKNOWN_NAME";
   }

   @Override
   public String type() {
      return "FS_UNKNOWN_TYPE";
   }

   @Override
   public boolean isReadOnly() {
      return false;
   }

   @Override
   public long getTotalSpace() throws IOException {
      return Long.MAX_VALUE;
   }

   @Override
   public long getUsableSpace() throws IOException {
      return Long.MAX_VALUE;
   }

   @Override
   public long getUnallocatedSpace() throws IOException {
      return Long.MAX_VALUE;
   }

   @Override
   public String toString() {
      return name();
   }

   /*
    * (non-Javadoc)
    *
    * @see java.nio.file.FileStore#supportsFileAttributeView(java.lang.Class)
    */
   @Override
   public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
      return false;
   }

   /*
    * (non-Javadoc)
    *
    * @see java.nio.file.FileStore#supportsFileAttributeView(java.lang.String)
    */
   @Override
   public boolean supportsFileAttributeView(String name) {
      return false;
   }

   /*
    * (non-Javadoc)
    *
    * @see java.nio.file.FileStore#getFileStoreAttributeView(java.lang.Class)
    */
   @Override
   public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
      return null;
   }

   /*
    * (non-Javadoc)
    *
    * @see java.nio.file.FileStore#getAttribute(java.lang.String)
    */
   @Override
   public Object getAttribute(String attribute) throws IOException {
      return null;
   }

}