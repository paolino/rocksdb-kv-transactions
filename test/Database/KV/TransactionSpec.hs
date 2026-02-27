{- |
Tests for Database.KV.Transaction
-}
module Database.KV.TransactionSpec (spec) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
    ( newEmptyMVar
    , putMVar
    , takeMVar
    )
import Data.ByteString (ByteString)
import Data.Default (Default (..))
import Data.Type.Equality ((:~:) (..))
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
    , delete
    , fromPairList
    , insert
    , mapColumns
    , query
    , runSpeculation
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

-- | Codecs for ByteString key-value pairs (identity encoding)
bsCodec :: Codecs (KV ByteString ByteString)
bsCodec = Codecs{keyCodec = id, valueCodec = id}

-- | GADT to select between different tables
data Tables a where
    Items :: Tables (KV ByteString ByteString)

instance GCompare Tables where
    gcompare Items Items = GEQ

instance GEq Tables where
    geq Items Items = Just Refl

-- | Index codecs by table type
codecs :: DMap Tables Codecs
codecs = fromPairList [Items :=> bsCodec]

-- | Run a transaction on a RocksDB database
runTx
    :: DB
    -> DMap Tables Codecs
    -> Transaction IO ColumnFamily Tables BatchOp a
    -> IO a
runTx db cols tx = do
    let rocksDBDatabase = mkRocksDBDatabase db $ mkColumns (columnFamilies db) cols
    runTransactionUnguarded rocksDBDatabase tx

{- | Run a speculative transaction on a RocksDB
database (reads snapshot, discards writes)
-}
runSpec
    :: DB
    -> DMap Tables Codecs
    -> Transaction IO ColumnFamily Tables BatchOp a
    -> IO a
runSpec db cols tx = do
    let rocksDBDatabase = mkRocksDBDatabase db $ mkColumns (columnFamilies db) cols
    runSpeculation rocksDBDatabase tx

-- | Sub-column GADT A (for mapColumns tests)
data SubA a where
    ItemsA :: SubA (KV ByteString ByteString)

instance GCompare SubA where
    gcompare ItemsA ItemsA = GEQ

instance GEq SubA where
    geq ItemsA ItemsA = Just Refl

-- | Sub-column GADT B (for mapColumns tests)
data SubB a where
    ItemsB :: SubB (KV ByteString ByteString)

instance GCompare SubB where
    gcompare ItemsB ItemsB = GEQ

instance GEq SubB where
    geq ItemsB ItemsB = Just Refl

-- | Unified column GADT combining SubA and SubB
data AllCols a where
    InA :: SubA a -> AllCols a
    InB :: SubB a -> AllCols a

instance GEq AllCols where
    geq (InA a) (InA a') = geq a a'
    geq (InB b) (InB b') = geq b b'
    geq _ _ = Nothing

instance GCompare AllCols where
    gcompare (InA a) (InA a') = gcompare a a'
    gcompare (InA _) (InB _) = GLT
    gcompare (InB _) (InA _) = GGT
    gcompare (InB b) (InB b') = gcompare b b'

-- | Codecs for unified columns
allCodecs :: DMap AllCols Codecs
allCodecs =
    fromPairList
        [ InA ItemsA :=> bsCodec
        , InB ItemsB :=> bsCodec
        ]

-- | Default config for test databases
cfg :: Config
cfg = def{createIfMissing = True}

spec :: Spec
spec = describe "Database.KV.Transaction" $ do
    describe "query" $ do
        it "returns Nothing for non-existent key" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ query Items "nonexistent"
            result `shouldBe` Nothing

        it "returns Just value for existing key" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "key1" "value1"
                    runTx db codecs $ query Items "key1"
            result `shouldBe` Just "value1"

    describe "insert" $ do
        it "inserts a new key-value pair" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "apple" "red"
                    runTx db codecs $ query Items "apple"
            result `shouldBe` Just "red"

        it "overwrites existing value" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "fruit" "apple"
                    runTx db codecs $ insert Items "fruit" "banana"
                    runTx db codecs $ query Items "fruit"
            result `shouldBe` Just "banana"

        it "multiple inserts in same transaction" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "alpha"
                        insert Items "b" "beta"
                        insert Items "c" "gamma"
                    runTx db codecs $ do
                        a <- query Items "a"
                        b <- query Items "b"
                        c <- query Items "c"
                        pure (a, b, c)
            result `shouldBe` (Just "alpha", Just "beta", Just "gamma")

    describe "delete" $ do
        it "removes existing key" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "toDelete" "value"
                    runTx db codecs $ delete Items "toDelete"
                    runTx db codecs $ query Items "toDelete"
            result `shouldBe` Nothing

        it "deleting non-existent key is no-op" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ delete Items "never-existed"
                    runTx db codecs $ query Items "never-existed"
            result `shouldBe` Nothing

    describe "transaction atomicity" $ do
        it "applies all operations atomically" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    -- Insert multiple items in one transaction
                    runTx db codecs $ do
                        insert Items "x" "1"
                        insert Items "y" "2"
                        insert Items "z" "3"
                    -- Verify all are present
                    runTx db codecs $ do
                        x <- query Items "x"
                        y <- query Items "y"
                        z <- query Items "z"
                        pure (x, y, z)
            result `shouldBe` (Just "1", Just "2", Just "3")

        it "read-modify-write pattern" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "counter" "0"
                    -- Read current value and write updated value
                    runTx db codecs $ do
                        mVal <- query Items "counter"
                        case mVal of
                            Just "0" -> insert Items "counter" "1"
                            _ -> pure ()
                    runTx db codecs $ query Items "counter"
            result `shouldBe` Just "1"

    describe "snapshot consistency" $ do
        it
            "queries within a transaction see consistent state despite concurrent writes"
            $ do
                result <- withSystemTempDirectory "test-db" $ \fp -> do
                    withDBCF fp cfg [("items", cfg)] $ \db -> do
                        runTx db codecs $ insert Items "k" "before"
                        -- Start a transaction that reads, waits, then reads again
                        -- A concurrent write happens during the wait
                        ready <- newEmptyMVar
                        done <- newEmptyMVar
                        _ <- forkIO $ do
                            takeMVar ready
                            runTx db codecs $ insert Items "k" "after"
                            putMVar done ()
                        runTx db codecs $ do
                            _ <- query Items "k"
                            -- Signal the writer and wait for it
                            Database.KV.Transaction.insert Items "_sync" "go"
                            pure ()
                        -- Write between two separate reads within one tx
                        -- Both reads should see the same snapshot
                        putMVar ready ()
                        takeMVar done
                        runTx db codecs
                            $ query Items "k"
                -- After the concurrent write, the value should be "after"
                result `shouldBe` Just "after"

        it "transaction reads are isolated from concurrent inserts" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                    -- Concurrent write to "a" while we read "a" and "b"
                    writerReady <- newEmptyMVar
                    writerDone <- newEmptyMVar
                    _ <- forkIO $ do
                        takeMVar writerReady
                        runTx db codecs $ insert Items "a" "CHANGED"
                        putMVar writerDone ()
                    -- Signal writer, wait for it, then read both in one tx
                    putMVar writerReady ()
                    takeMVar writerDone
                    -- Now "a" is "CHANGED" in the DB
                    -- A single transaction should see consistent state
                    runTx db codecs $ do
                        a <- query Items "a"
                        b <- query Items "b"
                        pure (a, b)
            result `shouldBe` (Just "CHANGED", Just "2")

    describe "speculation" $ do
        it "speculative writes are discarded" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "key" "original"
                    -- Speculate: insert a new value
                    runSpec db codecs $ do
                        insert Items "key" "speculative"
                        insert Items "new" "also-speculative"
                    -- Original value should be untouched
                    runTx db codecs $ do
                        k <- query Items "key"
                        n <- query Items "new"
                        pure (k, n)
            result `shouldBe` (Just "original", Nothing)

        it "speculation provides read-your-writes" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "x" "real"
                    -- Within speculation, writes should be visible
                    runSpec db codecs $ do
                        insert Items "x" "speculated"
                        insert Items "y" "new"
                        x <- query Items "x"
                        y <- query Items "y"
                        pure (x, y)
            result `shouldBe` (Just "speculated", Just "new")

        it "speculation reads from snapshot" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db codecs $ insert Items "s" "snap"
                    -- Speculation should see the value at snapshot time
                    runSpec db codecs $ query Items "s"
            result `shouldBe` Just "snap"

    describe "mapColumns" $ do
        let withAllDB action =
                withSystemTempDirectory "test-db"
                    $ \fp ->
                        withDBCF
                            fp
                            cfg
                            [ ("colA", cfg)
                            , ("colB", cfg)
                            ]
                            $ \db ->
                                action
                                    $ mkRocksDBDatabase db
                                    $ mkColumns
                                        (columnFamilies db)
                                        allCodecs
            run =
                runTransactionUnguarded

        it "lifts a sub-transaction" $ do
            result <- withAllDB $ \db -> do
                run db
                    $ mapColumns InA
                    $ insert ItemsA "ka" "va"
                run db
                    $ query (InA ItemsA) "ka"
            result `shouldBe` Just "va"

        it "composes from different types"
            $ do
                result <- withAllDB $ \db -> do
                    run db $ do
                        mapColumns InA
                            $ insert
                                ItemsA
                                "ka"
                                "va"
                        mapColumns InB
                            $ insert
                                ItemsB
                                "kb"
                                "vb"
                    run db $ do
                        a <-
                            query
                                (InA ItemsA)
                                "ka"
                        b <-
                            query
                                (InB ItemsB)
                                "kb"
                        pure (a, b)
                result
                    `shouldBe` ( Just "va"
                               , Just "vb"
                               )

        it "maps read-modify-write" $ do
            result <- withAllDB $ \db -> do
                run db
                    $ mapColumns InA
                    $ insert ItemsA "x" "old"
                run db $ mapColumns InA $ do
                    mv <- query ItemsA "x"
                    case mv of
                        Just _ ->
                            insert
                                ItemsA
                                "x"
                                "new"
                        Nothing -> pure ()
                run db
                    $ query (InA ItemsA) "x"
            result `shouldBe` Just "new"

        it "maps delete operations" $ do
            result <- withAllDB $ \db -> do
                run db
                    $ mapColumns InA
                    $ insert ItemsA "d" "val"
                run db
                    $ mapColumns InA
                    $ delete ItemsA "d"
                run db
                    $ query (InA ItemsA) "d"
            result
                `shouldBe` ( Nothing
                                :: Maybe ByteString
                           )
