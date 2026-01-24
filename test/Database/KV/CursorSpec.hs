{- |
Tests for Database.KV.Cursor
-}
module Database.KV.CursorSpec (spec) where

import Data.ByteString (ByteString)
import Data.Default (Default (..))
import Data.Type.Equality ((:~:) (..))
import Database.KV.Cursor
    ( Entry (..)
    , firstEntry
    , lastEntry
    , nextEntry
    , prevEntry
    , seekKey
    )
import Database.KV.Database (mkColumns)
import Database.KV.RocksDB (mkRocksDBDatabase)
import Database.KV.Transaction
    ( Codecs (..)
    , DMap
    , DSum (..)
    , GCompare (..)
    , GEq (..)
    , GOrdering (..)
    , KV
    , Transaction
    , insert
    , iterating
    , mkCols
    , runTransactionUnguarded
    )
import Database.RocksDB
    ( BatchOp
    , ColumnFamily
    , Config (createIfMissing)
    , DB (..)
    , withDBCF
    )
import System.IO.Temp (withSystemTempDirectory)
import Test.Hspec (Spec, describe, it, shouldBe)

-- | Codecs for ByteString key-value pairs
bsCodec :: Codecs (KV ByteString ByteString)
bsCodec = Codecs{keyCodec = id, valueCodec = id}

-- | Table selector
data Tables a where
    Items :: Tables (KV ByteString ByteString)

instance GCompare Tables where
    gcompare Items Items = GEQ

instance GEq Tables where
    geq Items Items = Just Refl

codecs :: DMap Tables Codecs
codecs = mkCols [Items :=> bsCodec]

runTx
    :: DB
    -> DMap Tables Codecs
    -> Transaction IO ColumnFamily Tables BatchOp a
    -> IO a
runTx db cols tx = do
    let rocksDBDatabase = mkRocksDBDatabase db $ mkColumns (columnFamilies db) cols
    runTransactionUnguarded rocksDBDatabase tx

cfg :: Config
cfg = def{createIfMissing = True}

spec :: Spec
spec = describe "Database.KV.Cursor" $ do
    describe "firstEntry / lastEntry" $ do
        it "returns Nothing on empty table" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ iterating Items firstEntry
            result `shouldBe` Nothing

        it "returns the only entry when table has one item" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "only" "one"
                    runTx db codecs $ iterating Items firstEntry
            result `shouldBe` Just Entry{entryKey = "only", entryValue = "one"}

        it "firstEntry returns lexicographically smallest key" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "banana" "yellow"
                        insert Items "apple" "red"
                        insert Items "cherry" "dark"
                    runTx db codecs $ iterating Items firstEntry
            result `shouldBe` Just Entry{entryKey = "apple", entryValue = "red"}

        it "lastEntry returns lexicographically largest key" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "banana" "yellow"
                        insert Items "apple" "red"
                        insert Items "cherry" "dark"
                    runTx db codecs $ iterating Items lastEntry
            result `shouldBe` Just Entry{entryKey = "cherry", entryValue = "dark"}

    describe "nextEntry / prevEntry" $ do
        it "iterates forward through all entries" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                        insert Items "c" "3"
                    runTx db codecs $ iterating Items $ do
                        e1 <- firstEntry
                        e2 <- nextEntry
                        e3 <- nextEntry
                        e4 <- nextEntry  -- Should be Nothing
                        pure (e1, e2, e3, e4)
            result
                `shouldBe` ( Just Entry{entryKey = "a", entryValue = "1"}
                           , Just Entry{entryKey = "b", entryValue = "2"}
                           , Just Entry{entryKey = "c", entryValue = "3"}
                           , Nothing
                           )

        it "iterates backward through all entries" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                        insert Items "c" "3"
                    runTx db codecs $ iterating Items $ do
                        e1 <- lastEntry
                        e2 <- prevEntry
                        e3 <- prevEntry
                        e4 <- prevEntry  -- Should be Nothing
                        pure (e1, e2, e3, e4)
            result
                `shouldBe` ( Just Entry{entryKey = "c", entryValue = "3"}
                           , Just Entry{entryKey = "b", entryValue = "2"}
                           , Just Entry{entryKey = "a", entryValue = "1"}
                           , Nothing
                           )

    describe "seekKey" $ do
        it "seeks to exact key when present" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                        insert Items "c" "3"
                    runTx db codecs $ iterating Items $ seekKey "b"
            result `shouldBe` Just Entry{entryKey = "b", entryValue = "2"}

        it "seeks to next key when exact key not present" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "1"
                        insert Items "c" "3"
                        insert Items "e" "5"
                    -- Seek to "b" should land on "c"
                    runTx db codecs $ iterating Items $ seekKey "b"
            result `shouldBe` Just Entry{entryKey = "c", entryValue = "3"}

        it "returns Nothing when seeking past all keys" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                    runTx db codecs $ iterating Items $ seekKey "z"
            result `shouldBe` Nothing

    describe "combined cursor operations" $ do
        it "seek then iterate" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                        insert Items "c" "3"
                        insert Items "d" "4"
                    runTx db codecs $ iterating Items $ do
                        e1 <- seekKey "b"
                        e2 <- nextEntry
                        e3 <- nextEntry
                        pure (e1, e2, e3)
            result
                `shouldBe` ( Just Entry{entryKey = "b", entryValue = "2"}
                           , Just Entry{entryKey = "c", entryValue = "3"}
                           , Just Entry{entryKey = "d", entryValue = "4"}
                           )
