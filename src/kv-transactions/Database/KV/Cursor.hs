{-# LANGUAGE UndecidableInstances #-}

{- |
Module      : Database.KV.Cursor
Description : Cursor-based iteration over key-value entries
Copyright   : (c) Paolo Veronelli, 2024
License     : Apache-2.0

This module provides cursor operations for iterating over entries in a
key-value column. Cursors maintain a position and can move forward,
backward, or seek to specific keys.

= Cursor Operations

* 'firstEntry' / 'lastEntry' - Move to first/last entry
* 'nextEntry' / 'prevEntry' - Move forward/backward
* 'seekKey' - Move to a specific key (or next key if not found)

= Resource Management

Cursors are automatically destroyed when the cursor program completes.
The underlying iterator resources are released via 'PosDestroy'.

= Example

@
-- Find all entries between two keys
rangeScan :: KeyOf c -> KeyOf c -> Cursor m c [Entry c]
rangeScan start end = do
    mFirst <- seekKey start
    case mFirst of
        Nothing -> pure []
        Just e -> collectUntil end [e]
  where
    collectUntil end acc = do
        mNext <- nextEntry
        case mNext of
            Nothing -> pure (reverse acc)
            Just e
                | entryKey e > end -> pure (reverse acc)
                | otherwise -> collectUntil end (e : acc)
@
-}
module Database.KV.Cursor
    ( -- * Cursor Entry
      Entry (..)

      -- * Cursor Instructions
    , Instruction (..)

      -- * Cursor Monad
    , Cursor

      -- * Cursor Operations
    , firstEntry
    , lastEntry
    , nextEntry
    , prevEntry
    , seekKey

      -- * Interpreter
    , interpretCursor
    )
where

import Control.Lens (preview, review)
import Control.Monad.Operational
    ( ProgramT
    , ProgramViewT (..)
    , singleton
    , viewT
    )
import Database.KV.Database
    ( Codecs (..)
    , Column (..)
    , KeyOf
    , Pos (..)
    , QueryIterator (..)
    , ValueOf
    )

-- | A key-value entry retrieved from the database.
data Entry c = Entry
    { entryKey :: KeyOf c
    -- ^ The entry's key
    , entryValue :: ValueOf c
    -- ^ The entry's value
    }

deriving instance (Show (KeyOf c), Show (ValueOf c)) => Show (Entry c)
deriving instance (Eq (KeyOf c), Eq (ValueOf c)) => Eq (Entry c)

-- | Low-level cursor instructions for navigation.
data Instruction c a where
    -- | Move to the first entry
    First :: Instruction c (Maybe (Entry c))
    -- | Move to the last entry
    Last :: Instruction c (Maybe (Entry c))
    -- | Move to the next entry
    Next :: Instruction c (Maybe (Entry c))
    -- | Move to the previous entry
    Prev :: Instruction c (Maybe (Entry c))
    -- | Seek to a specific key (or the next key if not found)
    Seek :: KeyOf c -> Instruction c (Maybe (Entry c))

{- | Cursor monad for composing navigation operations.
The @m@ parameter allows embedding in 'Transaction' or 'Querying'.
-}
type Cursor m c =
    ProgramT (Instruction c) m

{- | Move to the first entry in the column.
Returns @Nothing@ if the column is empty.
-}
firstEntry :: Cursor m c (Maybe (Entry c))
firstEntry = singleton First

{- | Move to the last entry in the column.
Returns @Nothing@ if the column is empty.
-}
lastEntry :: Cursor m c (Maybe (Entry c))
lastEntry = singleton Last

{- | Move to the next entry.
Returns @Nothing@ if already at the end.
-}
nextEntry :: Cursor m c (Maybe (Entry c))
nextEntry = singleton Next

{- | Move to the previous entry.
Returns @Nothing@ if already at the beginning.
-}
prevEntry :: Cursor m c (Maybe (Entry c))
prevEntry = singleton Prev

{- | Seek to the given key.
If the exact key doesn't exist, positions at the next key.
Returns @Nothing@ if no entries exist at or after the key.
-}
seekKey :: KeyOf c -> Cursor m c (Maybe (Entry c))
seekKey k = singleton $ Seek k

-- | Internal: Move iterator and decode entry if valid.
getIfValid
    :: (Monad m)
    => Codecs c
    -> QueryIterator m
    -> Pos
    -> m (Maybe (Entry c))
getIfValid codecs QueryIterator{isValid, entry, step} pos = do
    step pos
    valid <- isValid
    if valid
        then do
            me <- entry
            pure $ case me of
                Just (k, v) -> do
                    k' <- preview (keyCodec codecs) k
                    v' <- preview (valueCodec codecs) v
                    pure $ Entry{entryKey = k', entryValue = v'}
                Nothing -> Nothing
        else return Nothing

{- | Interpret a cursor program using a query iterator.
Automatically destroys the iterator when the program completes.
-}
interpretCursor
    :: forall m cf c a
     . (Monad m)
    => QueryIterator m
    -- ^ Backend iterator
    -> Column cf c
    -- ^ Column definition (for codecs)
    -> Cursor m c a
    -- ^ Cursor program to execute
    -> m a
interpretCursor qi@QueryIterator{step} column@Column{codecs} prog = do
    let get = getIfValid codecs qi
    v <- viewT prog
    case v of
        Return a -> do
            -- Release iterator resources
            step PosDestroy
            return a
        instr :>>= k -> do
            let cont = interpretCursor qi column . k
            cont =<< case instr of
                First -> get PosFirst
                Last -> get PosLast
                Next -> get PosNext
                Prev -> get PosPrev
                Seek key -> get $ PosAny $ review (keyCodec codecs) key
