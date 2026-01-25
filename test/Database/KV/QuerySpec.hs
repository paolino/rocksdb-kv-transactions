{- |
Tests for Database.KV.Query snapshot consistency.

These tests prove that all operations within a single 'interpretQuerying'
call see the same consistent snapshot, even when the database is being
modified concurrently by other threads.
-}
module Database.KV.QuerySpec (spec) where

import Control.Concurrent (forkIO, killThread, threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, try)
import Control.Monad (forM_)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS
import Data.Default (Default (..))
import Data.Type.Equality ((:~:) (..))
import Database.KV.Cursor
    ( Entry (..)
    , firstEntry
    , nextEntry
    )
import Database.KV.Database (mkColumns)
import Database.KV.Query
    ( Querying
    , interpretQuerying
    , iterating
    , query
    )
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
    , fromPairList
    , insert
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
codecs = fromPairList [Items :=> bsCodec]

cfg :: Config
cfg = def{createIfMissing = True}

-- | Run a transaction
runTx
    :: DB
    -> Transaction IO ColumnFamily Tables BatchOp a
    -> IO a
runTx db tx = do
    let rocksDBDatabase =
            mkRocksDBDatabase db $ mkColumns (columnFamilies db) codecs
    runTransactionUnguarded rocksDBDatabase tx

-- | Run a query
runQuery
    :: DB
    -> Querying IO ColumnFamily Tables BatchOp a
    -> IO a
runQuery db qry = do
    let rocksDBDatabase =
            mkRocksDBDatabase db $ mkColumns (columnFamilies db) codecs
    interpretQuerying rocksDBDatabase qry

-- | Collect all entries via iteration
collectAll
    :: Querying IO ColumnFamily Tables BatchOp [(ByteString, ByteString)]
collectAll = iterating Items go
  where
    go = do
        mEntry <- firstEntry
        case mEntry of
            Nothing -> pure []
            Just Entry{entryKey, entryValue} -> do
                rest <- goNext
                pure $ (entryKey, entryValue) : rest
    goNext = do
        mEntry <- nextEntry
        case mEntry of
            Nothing -> pure []
            Just Entry{entryKey, entryValue} -> do
                rest <- goNext
                pure $ (entryKey, entryValue) : rest

spec :: Spec
spec = describe "Database.KV.Query" $ do
    describe "basic querying" $ do
        it "can query a value" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db $ insert Items "key" "value"
                    runQuery db $ query Items "key"
            result `shouldBe` Just "value"

        it "can iterate entries" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    runTx db $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                    runQuery db collectAll
            result `shouldBe` [("a", "1"), ("b", "2")]

    describe "snapshot consistency" $ do
        it "repeated queries see the same value despite concurrent writes" $
            do
                result <- withSystemTempDirectory "test-db" $ \fp -> do
                    withDBCF fp cfg [("items", cfg)] $ \db -> do
                        -- Initial data
                        runTx db $ insert Items "key" "initial"

                        -- Signal to coordinate writer
                        started <- newEmptyMVar
                        done <- newEmptyMVar

                        -- Writer thread: rapidly update the value
                        writerThread <- forkIO $ do
                            putMVar started ()
                            let loop n
                                    | n > 200 = pure ()
                                    | otherwise = do
                                        let val = BS.pack $ "value-" <> show n
                                        runTx db $ insert Items "key" val
                                        threadDelay 500 -- 0.5ms between writes
                                        loop (n + 1 :: Int)
                            loop (0 :: Int)
                            putMVar done ()

                        -- Wait for writer to start
                        takeMVar started
                        threadDelay 1000 -- Let writer get going

                        -- Run slow query - all reads should see same value
                        queryResult <- runQuery db $ do
                            v1 <- query Items "key"
                            liftIO $ threadDelay 20000 -- 20ms delay
                            v2 <- query Items "key"
                            liftIO $ threadDelay 20000 -- 20ms delay
                            v3 <- query Items "key"
                            pure (v1, v2, v3)

                        -- Wait for writer or kill it
                        _ <- try (takeMVar done)
                            :: IO (Either SomeException ())
                        killThread writerThread

                        pure queryResult

                -- All three queries should return the same value
                let (v1, v2, v3) = result
                v1 `shouldBe` v2
                v2 `shouldBe` v3

        it "query and iterator see the same snapshot" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    -- Initial data: multiple keys
                    runTx db $ do
                        insert Items "a" "1"
                        insert Items "b" "2"
                        insert Items "c" "3"

                    started <- newEmptyMVar
                    done <- newEmptyMVar

                    -- Writer thread: modify all keys rapidly
                    writerThread <- forkIO $ do
                        putMVar started ()
                        let loop n
                                | n > 100 = pure ()
                                | otherwise = do
                                    let suffix = BS.pack $ "-v" <> show n
                                    runTx db $ do
                                        insert Items "a" ("1" <> suffix)
                                        insert Items "b" ("2" <> suffix)
                                        insert Items "c" ("3" <> suffix)
                                    threadDelay 500
                                    loop (n + 1 :: Int)
                        loop (0 :: Int)
                        putMVar done ()

                    takeMVar started
                    threadDelay 1000

                    -- Query and iterate - should see consistent state
                    queryResult <- runQuery db $ do
                        -- Point query
                        vA <- query Items "a"
                        liftIO $ threadDelay 15000 -- 15ms

                        -- Iterate all entries
                        allEntries <- collectAll
                        liftIO $ threadDelay 15000 -- 15ms

                        -- Query again
                        vA2 <- query Items "a"
                        vB <- query Items "b"
                        vC <- query Items "c"

                        pure (vA, vA2, vB, vC, allEntries)

                    _ <- try (takeMVar done) :: IO (Either SomeException ())
                    killThread writerThread

                    pure queryResult

            let (vA, vA2, vB, vC, allEntries) = result

            -- Point queries should be consistent
            vA `shouldBe` vA2

            -- Iterator should see same values as point queries
            lookup "a" allEntries `shouldBe` vA
            lookup "b" allEntries `shouldBe` vB
            lookup "c" allEntries `shouldBe` vC

        it "multiple iterations see the same data" $ do
            result <- withSystemTempDirectory "test-db" $ \fp -> do
                withDBCF fp cfg [("items", cfg)] $ \db -> do
                    -- Initial data
                    runTx db $ do
                        forM_ [1 .. 10 :: Int] $ \i ->
                            insert
                                Items
                                (BS.pack $ show i)
                                (BS.pack $ "val" <> show i)

                    started <- newEmptyMVar
                    done <- newEmptyMVar

                    -- Writer thread
                    writerThread <- forkIO $ do
                        putMVar started ()
                        let loop n
                                | n > 150 = pure ()
                                | otherwise = do
                                    forM_ [1 .. 10 :: Int] $ \i -> do
                                        let val =
                                                BS.pack
                                                    $ "val"
                                                    <> show i
                                                    <> "-"
                                                    <> show n
                                        runTx db
                                            $ insert Items (BS.pack $ show i) val
                                    threadDelay 300
                                    loop (n + 1 :: Int)
                        loop (0 :: Int)
                        putMVar done ()

                    takeMVar started
                    threadDelay 1000

                    -- Multiple iterations should see the same data
                    queryResult <- runQuery db $ do
                        entries1 <- collectAll
                        liftIO $ threadDelay 20000
                        entries2 <- collectAll
                        liftIO $ threadDelay 20000
                        entries3 <- collectAll
                        pure (entries1, entries2, entries3)

                    _ <- try (takeMVar done) :: IO (Either SomeException ())
                    killThread writerThread

                    pure queryResult

            let (entries1, entries2, entries3) = result
            entries1 `shouldBe` entries2
            entries2 `shouldBe` entries3
