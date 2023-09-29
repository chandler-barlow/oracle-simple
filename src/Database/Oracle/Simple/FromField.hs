{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeSynonymInstances #-}

module Database.Oracle.Simple.FromField where

import Control.Exception
import Control.Monad
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as B
import Data.Coerce
import Data.Fixed
import Data.Functor ((<&>))
import Data.Int
import qualified Data.List as L
import Data.Maybe (fromMaybe)
import Data.Proxy
import Data.Text
import Data.Text.Encoding
import Data.Time
import Data.Word
import Database.Oracle.Simple.Internal
import Foreign (peekArray)
import Foreign.C.String
import Foreign.C.Types
import Foreign.Ptr
import Foreign.Storable.Generic
import GHC.Generics

-- | A type that may be parsed from a database field.
class (HasDPINativeType a) => FromField a where
  fromField :: FieldParser a

instance Functor FieldParser where
  fmap f FieldParser {..} = FieldParser (fmap f <$> readDPIDataBuffer)

instance FromField Double where
  fromField = FieldParser getDouble

instance FromField Float where
  fromField = FieldParser getFloat

instance FromField DPITimestamp where
  fromField = FieldParser getTimestamp

instance FromField Text where
  fromField = FieldParser getText

instance FromField String where
  fromField = FieldParser getString

instance FromField Int64 where
  fromField = FieldParser getInt64

instance FromField Word64 where
  fromField = FieldParser getWord64

instance FromField Bool where
  fromField = FieldParser getBool

instance FromField Int where
  fromField = fromIntegral <$> fromField @Int64

instance (FromField a) => FromField (Maybe a) where
  fromField = FieldParser $ \ptr -> do
    result <- dpiData_getIsNull ptr
    if result == 1
      then pure Nothing
      else Just <$> readDPIDataBuffer (fromField @a) ptr

instance FromField UTCTime where
  fromField = dpiTimeStampToUTCTime <$> fromField

dpiTimeStampToUTCTime :: DPITimestamp -> UTCTime
dpiTimeStampToUTCTime dpi =
  let DPITimestamp {..} = dpiTimeStampToUTCDPITimeStamp dpi
      local = LocalTime d tod
      d = fromGregorian (fromIntegral year) (fromIntegral month) (fromIntegral day)
      tod = TimeOfDay (fromIntegral hour) (fromIntegral minute) (fromIntegral second + picos)
      picos = MkFixed (fromIntegral fsecond * 1000) :: Pico
   in localTimeToUTC utc local

-- | Encapsulates all information needed to parse a field as a Haskell value.
newtype FieldParser a = FieldParser
  { -- | A function that retrieves a value of type @a@ from the DPI data buffer.
    readDPIDataBuffer :: ReadDPIBuffer a
  }

instance Applicative FieldParser where
  pure x = FieldParser $ \ptr -> pure x
  FieldParser f <*> FieldParser g = FieldParser $ \ptr -> do
    f' <- f ptr
    x <- g ptr
    pure (f' x)

instance Monad FieldParser where
  FieldParser g >>= f = FieldParser $ \ptr -> do
    x <- g ptr
    readDPIDataBuffer (f x) ptr

-- | Alias for a function that retrieves a value of type @a@ from the DPI data buffer
type ReadDPIBuffer a = Ptr (DPIData ReadBuffer) -> IO a

-- ** @ReadDPIBuffer@s for common types

-- | Get a Double value from the data buffer
getDouble :: ReadDPIBuffer Double
getDouble = coerce <$> dpiData_getDouble

-- | Get a Float value from the data buffer
getFloat :: ReadDPIBuffer Float
getFloat = coerce <$> dpiData_getFloat

-- | Get an Int64 value from the data buffer.
getInt64 :: ReadDPIBuffer Int64
getInt64 = dpiData_getInt64

-- | Get a Word64 value from the data buffer.
getWord64 :: ReadDPIBuffer Word64
getWord64 = dpiData_getUint64

-- | Get a boolean value from the data buffer.
getBool :: ReadDPIBuffer Bool
getBool ptr = (== 1) <$> dpiData_getBool ptr

-- | Get Text from the data buffer.
-- Supports ASCII, UTF-8 and UTF-16 big- and little-endian encodings.
-- Throws 'FieldParseError' if any other encoding is encountered.
getText :: ReadDPIBuffer Text
getText = buildText <=< peek <=< dpiData_getBytes
  where
    buildText DPIBytes {..} = do
      gotBytes <- BS.packCStringLen (dpiBytesPtr, fromIntegral dpiBytesLength)
      encoding <- peekCString dpiBytesEncoding
      decodeFn <- case encoding of
        "ASCII" -> pure decodeASCII
        "UTF-8" -> pure decodeUtf8
        "UTF-16BE" -> pure decodeUtf16BE
        "UTF-16LE" -> pure decodeUtf16LE
        otherEnc -> throwIO $ UnsupportedEncoding otherEnc
      evaluate (decodeFn gotBytes)
        `catch` ( \(e :: SomeException) -> throwIO (ByteDecodeError encoding (displayException e))
                )

-- | Get Text from the data buffer
getString :: ReadDPIBuffer String
getString = fmap unpack <$> getText

-- | Get a `DPITimestamp` from the buffer
getTimestamp :: ReadDPIBuffer DPITimestamp
getTimestamp = peek <=< dpiData_getTimestamp

-- The main function for dealing with json
getJson :: ReadDPIBuffer DpiJsonNode
getJson = peek <=< dpiData_getJson

-- if the node type is json array use this
getJsonArray :: ReadDPIBuffer DpiJsonArray
getJsonArray = peek <=< dpiData_getJsonArray

-- if the node type is json object use this
getJsonObject :: ReadDPIBuffer DpiJsonObject
getJsonObject = peek <=< dpiData_getJsonObject

instance FromField DpiJsonNode where
  fromField = FieldParser getJson

instance FromField DpiJsonObject where
  fromField = FieldParser getJsonObject

instance FromField DpiJsonArray where
  fromField = FieldParser getJsonArray

instance FromField JsonByteString where
  fromField = FieldParser (getJson >=> dpiJsonNodeToJNode >=> toBs)
    where
      toBs :: (Show a) => a -> IO B.ByteString
      toBs = return . B.pack . show

-- Defaults to a null value for unparseble json types
-- may not be good behaviour
dpiJsonNodeToJNode :: DpiJsonNode -> IO JNode
dpiJsonNodeToJNode DpiJsonNode {..} = do
  let ntype = fromMaybe DPI_NATIVE_TYPE_NULL (uintToDPINativeType nodeNativeTypeNum)
  toJNode ntype nodeValue

dpiJsonObjectToJNode :: DpiJsonObject -> IO JNode
dpiJsonObjectToJNode DpiJsonObject {..} = do
  let n = fromIntegral objNumFields
  fieldNames <- mapM peekCString =<< peekArray n objFieldNames
  fields <- mapM dpiJsonNodeToJNode =<< peekArray n objFields
  return $ JObject (L.zip fieldNames fields)

dpiJsonArrayToJNode :: DpiJsonArray -> IO JNode
dpiJsonArrayToJNode DpiJsonArray {..} = do
  let n = fromIntegral arrNumElements
  xs <- mapM dpiJsonNodeToJNode =<< peekArray n arrElements
  return $ JArray xs

toJNode :: DPINativeType -> Ptr (DPIData ReadBuffer) -> IO JNode
toJNode ntype b = case ntype of
  DPI_NATIVE_TYPE_INT64 -> getInt64 b <&> (JNumber . toRational)
  DPI_NATIVE_TYPE_FLOAT -> getFloat b <&> (JNumber . toRational)
  DPI_NATIVE_TYPE_DOUBLE -> getDouble b <&> (JNumber . toRational)
  DPI_NATIVE_TYPE_BOOLEAN -> getBool b <&> JBool
  DPI_NATIVE_TYPE_NULL -> return JNull
  DPI_NATIVE_TYPE_JSON -> getJson b >>= dpiJsonNodeToJNode
  DPI_NATIVE_TYPE_JSON_OBJECT -> getJsonObject b >>= dpiJsonObjectToJNode
  DPI_NATIVE_TYPE_JSON_ARRAY -> getJsonArray b >>= dpiJsonArrayToJNode

data JNode
  = JString String
  | JArray [JNode]
  | JNumber Rational
  | JObject [(String, JNode)]
  | JBool Bool
  | JNull

instance Show JNode where
  show (JString s) = "\\\"" ++ s ++ "\\\""
  show (JBool b) = if b then "true" else "false"
  show (JNumber n) = show n
  show (JArray arr) =
    L.concat
      [ "[",
        L.intercalate "," (L.map show arr),
        "]"
      ]
  show (JObject obj) =
    L.concat
      [ "{",
        L.intercalate "," (L.map (\(k, v) -> L.concat [k, ":", show v]) obj),
        "}"
      ]

-- | Errors encountered when parsing a database field.
data FieldParseError
  = -- | We encountered an encoding other than ASCII, UTF-8 or UTF-16
    UnsupportedEncoding {fpeOtherEncoding :: String}
  | -- | Failed to decode bytes using stated encoding
    ByteDecodeError {fpeEncoding :: String, fpeErrorMsg :: String}
  deriving (Show)

instance Exception FieldParseError where
  displayException UnsupportedEncoding {..} =
    "Field Parse Error: Encountered unsupported text encoding '"
      <> fpeOtherEncoding
      <> "'. Supported encodings: ASCII, UTF-8, UTF-16BE, UTF-16LE."
  displayException ByteDecodeError {..} =
    "Field Parse Error: Failed to decode bytes as " <> fpeEncoding <> ": " <> fpeErrorMsg
