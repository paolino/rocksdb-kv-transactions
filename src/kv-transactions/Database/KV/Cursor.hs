{-# LANGUAGE UndecidableInstances #-}

module Database.KV.Cursor
    ( -- * Cursor instructions
      Instruction (..)

      -- * Cursor operational monad
    , Cursor
    , Entry (..)

      -- * Cursor operations
    , firstEntry
    , lastEntry
    , nextEntry
    , prevEntry
    , seekKey

      -- * Cursor interpreter
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

-- | Represents a key-value entry in the database.
data Entry c = Entry
    { entryKey :: KeyOf c
    , entryValue :: ValueOf c
    }

deriving instance (Show (KeyOf c), Show (ValueOf c)) => Show (Entry c)
deriving instance (Eq (KeyOf c), Eq (ValueOf c)) => Eq (Entry c)

-- | Instructions for cursor operations within a transaction.
data Instruction c a where
    First :: Instruction c (Maybe (Entry c))
    Last :: Instruction c (Maybe (Entry c))
    Next :: Instruction c (Maybe (Entry c))
    Prev :: Instruction c (Maybe (Entry c))
    Seek :: KeyOf c -> Instruction c (Maybe (Entry c))

-- | Cursor operational monad
type Cursor m c =
    ProgramT (Instruction c) m

firstEntry :: Cursor m c (Maybe (Entry c))
firstEntry = singleton First

lastEntry :: Cursor m c (Maybe (Entry c))
lastEntry = singleton Last

nextEntry :: Cursor m c (Maybe (Entry c))
nextEntry = singleton Next

prevEntry :: Cursor m c (Maybe (Entry c))
prevEntry = singleton Prev

seekKey :: KeyOf c -> Cursor m c (Maybe (Entry c))
seekKey k = singleton $ Seek k

getIfValid
    :: Monad m
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

interpretCursor
    :: forall m cf c a
     . Monad m
    => QueryIterator m
    -> Column cf c
    -> Cursor m c a
    -> m a
interpretCursor qi@QueryIterator{step} column@Column{codecs} prog = do
    let get = getIfValid codecs qi
    v <- viewT prog
    case v of
        Return a -> do
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
