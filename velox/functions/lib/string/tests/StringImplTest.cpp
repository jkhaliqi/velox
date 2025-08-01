/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/functions/lib/string/StringImpl.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/type/StringView.h"

#include <gtest/gtest.h>
#include <memory>
#include <vector>

using namespace facebook::velox;
using namespace facebook::velox::functions::stringImpl;
using namespace facebook::velox::functions::stringCore;

class StringImplTest : public testing::Test {
 public:
  std::vector<std::tuple<std::string, std::string>> getUpperAsciiTestData() {
    return {
        {"abcdefg", "ABCDEFG"},
        {"ABCDEFG", "ABCDEFG"},
        {"a B c D e F g", "A B C D E F G"},
    };
  }

  std::vector<std::tuple<std::string, std::string>> getUpperUnicodeTestData() {
    return {
        {"àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ", "ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ"},
        {"αβγδεζηθικλμνξοπρςστυφχψ", "ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ"},
        {"абвгдежзийклмнопрстуфхцчшщъыьэюя",
         "АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ"},
        {"\u0069", "\u0049"},
        {"\u03C3", "\u03A3"},
        {"i\xCC\x87", "I\xCC\x87"},
        {"\u010B", "\u010A"},
        {"\u0117", "\u0116"},
        {"\u0121", "\u0120"},
        {"\u017C", "\u017B"},
        {"\u0227", "\u0226"},
        {"\u022F", "\u022E"},
        {"\u1E03", "\u1E02"},
        {"\u1E0B", "\u1E0A"},
        {"\u1E1F", "\u1E1E"},
        {"\u1E23", "\u1E22"},
        {"\u1E41", "\u1E40"},
        {"\u1E45", "\u1E44"},
        {"\u1E57", "\u1E56"},
        {"\u1E59", "\u1E58"},
        {"\u1E61", "\u1E60"},
        {"\u1E65", "\u1E64"},
        {"\u1E67", "\u1E66"},
        {"\u1E69", "\u1E68"},
        {"\u1E6B", "\u1E6A"},
        {"\u1E87", "\u1E86"},
        {"\u1E8B", "\u1E8A"},
        {"\u1E8F", "\u1E8E"},
        {"πας", "ΠΑΣ"},
        {"παςa", "ΠΑΣA"},
    };
  }

  std::vector<std::tuple<std::string, std::string>> getLowerAsciiTestData() {
    return {
        {"ABCDEFG", "abcdefg"},
        {"abcdefg", "abcdefg"},
        {"a B c D e F g", "a b c d e f g"},
    };
  }

  std::vector<std::tuple<std::string, std::string>> getLowerUnicodeTestData() {
    return {
        {"ÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞ", "àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ"},
        {"ΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΣΤΥΦΧΨ", "αβγδεζηθικλμνξοπρσστυφχψ"},
        {"АБВГДЕЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ",
         "абвгдежзийклмнопрстуфхцчшщъыьэюя"},
        {"\u0130", "\u0069"},
        {"\u03A3", "\u03C3"},
        {"I\xCC\x87", "i\xCC\x87"},
        {"\u010A", "\u010B"},
        {"\u0116", "\u0117"},
        {"\u0120", "\u0121"},
        {"\u017B", "\u017C"},
        {"\u0226", "\u0227"},
        {"\u022E", "\u022F"},
        {"\u1E02", "\u1E03"},
        {"\u1E0A", "\u1E0B"},
        {"\u1E1E", "\u1E1F"},
        {"\u1E22", "\u1E23"},
        {"\u1E40", "\u1E41"},
        {"\u1E44", "\u1E45"},
        {"\u1E56", "\u1E57"},
        {"\u1E58", "\u1E59"},
        {"\u1E60", "\u1E61"},
        {"\u1E64", "\u1E65"},
        {"\u1E66", "\u1E67"},
        {"\u1E68", "\u1E69"},
        {"\u1E6A", "\u1E6B"},
        {"\u1E86", "\u1E87"},
        {"\u1E8A", "\u1E8B"},
        {"\u1E8E", "\u1E8F"},
        {"ΠΑΣ", "πασ"},
        {"ΠΑΣA", "πασa"},
    };
  }

  static std::vector<std::pair<std::string, std::string>>
  getInitcapUnicodePrestoTestData() {
    return {
        {"BİLGİ", "Bilgi"},
        {"\u0130\u0130", "\u0130\u0069"},
        {"foo\u0020bar", "Foo\u0020Bar"},
        {"foo\u0009bar", "Foo\u0009Bar"},
        {"foo\u000Abar", "Foo\u000ABar"},
        {"foo\u000Dbar", "Foo\u000DBar"},
        {"foo\u000Bbar", "Foo\u000BBar"},
        {"foo\u000Cbar", "Foo\u000CBar"},
        {"foo\u0009\u000A\u000D\u000B\u000Cbar",
         "Foo\u0009\u000A\u000D\u000B\u000CBar"},
        {"foo\u0020\u0009\u000Abar", "Foo\u0020\u0009\u000ABar"},
        {"foo\u1680bar", "Foo\u1680Bar"},
        {"foo\u2000bar", "Foo\u2000Bar"},
        {"foo\u2001bar", "Foo\u2001Bar"},
        {"foo\u2002bar", "Foo\u2002Bar"},
        {"foo\u2003bar", "Foo\u2003Bar"},
        {"foo\u2004bar", "Foo\u2004Bar"},
        {"foo\u2005bar", "Foo\u2005Bar"},
        {"foo\u2006bar", "Foo\u2006Bar"},
        {"foo\u2008bar", "Foo\u2008Bar"},
        {"foo\u2009bar", "Foo\u2009Bar"},
        {"foo\u200Abar", "Foo\u200ABar"},
        {"foo\u2028bar", "Foo\u2028Bar"},
        {"foo\u2029bar", "Foo\u2029Bar"},
        {"foo\u205Fbar", "Foo\u205FBar"},
        {"foo\u3000bar", "Foo\u3000Bar"},
        {"foo\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2008\u2009\u200Abar",
         "Foo\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2008\u2009\u200ABar"},
        {"\u00E9l\u00E8ve\u000Atr\u00E8s-intelligent",
         "\u00C9l\u00E8ve\u000ATr\u00E8s-intelligent"},
        // Below whitespaces are not considered as whitespace in presto
        {"foo\u0085Bar", "Foo\u0085bar"},
        {"foo\u00A0Bar", "Foo\u00A0bar"},
        {"foo\u2007Bar", "Foo\u2007bar"},
    };
  }

  static std::vector<std::pair<std::string, std::string>>
  getInitcapAsciiPrestoTestData() {
    return {
        {"foo bar", "Foo Bar"},
        {"foo\nbar", "Foo\nBar"},
        {"foo \t\nbar", "Foo \t\nBar"}};
  }

  static std::vector<std::pair<std::string, std::string>>
  getInitcapUnicodeSparkTestData() {
    return {
        {"àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ", "Àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþ"},
        {"αβγδεζηθικλμνξοπρςστυφχψ", "Αβγδεζηθικλμνξοπρςστυφχψ"},
        {"абвгдежзийклмнопрстуфхцчшщъыьэюя",
         "Абвгдежзийклмнопрстуфхцчшщъыьэюя"},
        {"hello world", "Hello World"},
        {"HELLO WORLD", "Hello World"},
        {"1234", "1234"},
        {"1234", "1234"},
        {"", ""},
        {"élève très-intelligent", "Élève Très-intelligent"},
        {"mañana-por_la_tarde!", "Mañana-por_la_tarde!"},
        {"добро-пожаловать.тест", "Добро-пожаловать.тест"},
        {"çalışkan öğrenci@üniversite.tr", "Çalışkan Öğrenci@üniversite.tr"},
        {"emoji😊test🚀case", "Emoji😊test🚀case"},
        {"тест@пример.рф", "Тест@пример.рф"},
        {"BİLGİ", "Bi̇lgi̇"},
        {"\u0130\u0130", "\u0130\u0069\u0307"},
        {"İstanbul", "İstanbul"}};
  }

  static std::vector<std::pair<std::string, std::string>>
  getInitcapAsciiSparkTestData() {
    return {
        {"abcdefg", "Abcdefg"},
        {" abcdefg", " Abcdefg"},
        {" abc defg", " Abc Defg"},
        {"ABCDEFG", "Abcdefg"},
        {"a B c D e F g", "A B C D E F G"},
        {"hello world", "Hello World"},
        {"HELLO WORLD", "Hello World"},
        {"1234", "1234"},
        {"", ""},
        {"urna.Ut@egetdictumplacerat.edu", "Urna.ut@egetdictumplacerat.edu"},
        {"nibh.enim@egestas.ca", "Nibh.enim@egestas.ca"},
        {"in@Donecat.ca", "In@donecat.ca"},
        {"sodales@blanditviverraDonec.ca", "Sodales@blanditviverradonec.ca"},
        {"sociis.natoque.penatibus@vitae.org",
         "Sociis.natoque.penatibus@vitae.org"},
        {"john_doe-123@example-site.com", "John_doe-123@example-site.com"},
        {"MIXED.case-EMAIL_42@domain.NET", "Mixed.case-email_42@domain.net"},
        {"...weird..case@@", "...weird..case@@"},
        {"user-name+filter@sub.mail.org", "User-name+filter@sub.mail.org"},
        {"CAPS_LOCK@DOMAIN.COM", "Caps_lock@domain.com"},
        {"__init__.py@example.dev", "__init__.py@example.dev"}};
  }
};

TEST_F(StringImplTest, upperAscii) {
  for (auto& testCase : getUpperAsciiTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedUpper = std::get<1>(testCase);

    std::string upperOutput;
    upper</*ascii*/ true>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerAscii) {
  for (auto& testCase : getLowerAsciiTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower</*ascii*/ true>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
}

TEST_F(StringImplTest, upperUnicode) {
  for (auto& testCase : getUpperUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedUpper = std::get<1>(testCase);

    std::string upperOutput;
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);

    upperOutput.clear();
    upper</*ascii*/ false>(upperOutput, input);
    ASSERT_EQ(upperOutput, expectedUpper);
  }
}

TEST_F(StringImplTest, lowerUnicode) {
  for (auto& testCase : getLowerUnicodeTestData()) {
    auto input = StringView(std::get<0>(testCase));
    auto& expectedLower = std::get<1>(testCase);

    std::string lowerOutput;
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);

    lowerOutput.clear();
    lower</*ascii*/ false>(lowerOutput, input);
    ASSERT_EQ(lowerOutput, expectedLower);
  }
}

TEST_F(StringImplTest, concatLazy) {
  core::StringWriter output;

  // concat(lower(in1), upper(in2));
  auto f1 = [&](core::StringWriter& out) {
    std::string input("AA");
    out.reserve(out.size() + input.size());
    lowerAscii(out.data() + out.size(), input.data(), input.size());
    out.resize(out.size() + input.size());
  };

  auto f2 = [&](core::StringWriter& out) {
    std::string input("bb");
    out.reserve(out.size() + input.size());
    upperAscii(out.data() + out.size(), input.data(), input.size());
    out.resize(out.size() + input.size());
  };

  concatLazy(output, f1, f2);
  ASSERT_EQ(StringView("aaBB"), output);
}

TEST_F(StringImplTest, length) {
  auto lengthUtf8Ref = [](const char* inputBuffer, size_t bufferLength) {
    size_t size = 0;
    for (size_t i = 0; i < bufferLength; i++) {
      if ((static_cast<const unsigned char>(inputBuffer[i]) & 0xC0) != 0x80) {
        size++;
      }
    }
    return size;
  };

  // Test ascii inputs
  for (const auto& test : getUpperAsciiTestData()) {
    auto& inputString = std::get<0>(test);

    ASSERT_EQ(length</*isAscii*/ true>(inputString), inputString.size());
    ASSERT_EQ(length</*isAscii*/ false>(inputString), inputString.size());
    ASSERT_EQ(length</*isAscii*/ false>(inputString), inputString.size());
  }

  // Test unicode inputs
  for (auto& test : getLowerUnicodeTestData()) {
    auto& inputString = std::get<0>(test);

    ASSERT_EQ(
        length</*isAscii*/ false>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
    ASSERT_EQ(
        length</*isAscii*/ false>(inputString),
        lengthUtf8Ref(inputString.data(), inputString.size()));
  }
}

TEST_F(StringImplTest, cappedLength) {
  auto input = std::string("abcd");
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 1), 1);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 2), 2);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 3), 3);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 4), 4);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 5), 4);
  ASSERT_EQ(cappedLength</*isAscii*/ true>(input, 6), 4);

  input = std::string("你好a世界");
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 1), 1);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 2), 2);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 3), 3);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 4), 4);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 5), 5);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 6), 5);
  ASSERT_EQ(cappedLength</*isAscii*/ false>(input, 7), 5);
}

TEST_F(StringImplTest, cappedUnicodeBytes) {
  // Test functions use case for indexing
  // UTF strings.
  std::string stringInput = "\xF4\x90\x80\x80Hello";
  ASSERT_EQ('H', stringInput[cappedByteLength<false>(stringInput, 2) - 1]);
  ASSERT_EQ('e', stringInput[cappedByteLength<false>(stringInput, 3) - 1]);
  ASSERT_EQ('l', stringInput[cappedByteLength<false>(stringInput, 4) - 1]);
  ASSERT_EQ('l', stringInput[cappedByteLength<false>(stringInput, 5) - 1]);
  ASSERT_EQ('o', stringInput[cappedByteLength<false>(stringInput, 6) - 1]);
  ASSERT_EQ('o', stringInput[cappedByteLength<false>(stringInput, 7) - 1]);

  // Multi-byte chars
  stringInput = "♫¡Singing is fun!♫";
  auto sPos = cappedByteLength<false>(stringInput, 2);
  auto exPos = cappedByteLength<false>(stringInput, 17);
  ASSERT_EQ("Singing is fun!♫", stringInput.substr(sPos));
  ASSERT_EQ("♫¡Singing is fun!", stringInput.substr(0, exPos));
  ASSERT_EQ("Singing is fun!", stringInput.substr(sPos, exPos - sPos));

  stringInput = std::string("abcd");
  auto stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 1), 1);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 2), 2);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 3), 3);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 4), 4);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 5), 4);
  ASSERT_EQ(cappedByteLength<true>(stringInput, 6), 4);

  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 1), 1);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 2), 2);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 3), 3);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 4), 4);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 5), 4);
  ASSERT_EQ(cappedByteLength<true>(stringViewInput, 6), 4);

  stringInput = std::string("你好a世界");
  stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 3);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 2), 6);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 3), 7);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 4), 10);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 5), 13);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 6), 13);

  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 1), 3);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 2), 6);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 3), 7);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 4), 10);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 5), 13);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 6), 13);

  stringInput = std::string("\x80");
  stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 2), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 3), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 4), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 5), 1);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 6), 1);

  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 1), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 2), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 3), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 4), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 5), 1);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 6), 1);

  stringInput.resize(2);
  // Create corrupt data below.
  char16_t c = u'\u04FF';
  stringInput[0] = (char)c;
  stringInput[1] = (char)c;

  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 1);

  stringInput.resize(4);
  c = u'\u04F4';
  char16_t c2 = u'\u048F';
  char16_t c3 = u'\u04BF';
  stringInput[0] = (char)c;
  stringInput[1] = (char)c2;
  stringInput[2] = (char)c3;
  stringInput[3] = (char)c3;

  stringViewInput = std::string_view(stringInput);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 1), 4);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 2), 4);
  ASSERT_EQ(cappedByteLength<false>(stringInput, 3), 4);

  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 1), 4);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 2), 4);
  ASSERT_EQ(cappedByteLength<false>(stringViewInput, 3), 4);
}

TEST_F(StringImplTest, badUnicodeLength) {
  ASSERT_EQ(0, length</*isAscii*/ false>(std::string("")));
  ASSERT_EQ(2, length</*isAscii*/ false>(std::string("ab")));
  // Try a bunch of special case unicode chars
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\u04FF")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U000E002F")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U0001D437")));
  ASSERT_EQ(1, length</*isAscii*/ false>(std::string("\U00002799")));

  std::string str;
  str.resize(2);
  // Create corrupt data below.
  char16_t c = u'\u04FF';
  str[0] = (char)c;
  str[1] = (char)c;

  auto len = length</*isAscii*/ false>(str);
  ASSERT_EQ(2, len);
}

TEST_F(StringImplTest, codePointToString) {
  auto testValidInput = [](const int64_t codePoint,
                           const std::string& expectedString) {
    core::StringWriter output;
    codePointToString(output, codePoint);
    ASSERT_EQ(
        StringView(expectedString), StringView(output.data(), output.size()));
  };

  auto testInvalidCodePoint = [](const int64_t codePoint) {
    core::StringWriter output;
    EXPECT_THROW(codePointToString(output, codePoint), VeloxUserError)
        << "codePoint " << codePoint;
  };

  testValidInput(65, "A");
  testValidInput(9731, "\u2603");
  testValidInput(0, std::string("\0", 1));

  testInvalidCodePoint(-1);
  testInvalidCodePoint(1234567);
  testInvalidCodePoint(8589934592);
}

TEST_F(StringImplTest, charToCodePoint) {
  auto testValidInput = [](const std::string& charString,
                           const int64_t expectedCodePoint) {
    ASSERT_EQ(charToCodePoint(StringView(charString)), expectedCodePoint);
  };

  auto testValidInputRoundTrip = [](const int64_t codePoint) {
    core::StringWriter string;
    codePointToString(string, codePoint);
    ASSERT_EQ(charToCodePoint(string), codePoint) << "codePoint " << codePoint;
  };

  auto testExpectDeath = [](const std::string& charString) {
    EXPECT_THROW(charToCodePoint(StringView(charString)), VeloxUserError)
        << "charString " << charString;
  };

  testValidInput("x", 0x78);
  testValidInput("\u840C", 0x840C);

  testValidInputRoundTrip(128077);
  testValidInputRoundTrip(33804);

  testExpectDeath("hello");
  testExpectDeath("\u666E\u5217\u65AF\u6258");
  testExpectDeath("");
}

TEST_F(StringImplTest, stringToCodePoints) {
  auto testStringToCodePoints =
      [](const std::string& charString,
         const std::vector<int32_t>& expectedCodePoints) {
        std::vector<int32_t> codePoints = stringToCodePoints(charString);
        ASSERT_EQ(codePoints.size(), expectedCodePoints.size());
        for (int i = 0; i < codePoints.size(); i++) {
          ASSERT_EQ(codePoints.at(i), expectedCodePoints.at(i));
        }
      };

  testStringToCodePoints("", {});
  testStringToCodePoints("h", {0x0068});
  testStringToCodePoints("hello", {0x0068, 0x0065, 0x006C, 0x006C, 0x006F});

  testStringToCodePoints("hïllo", {0x0068, 0x00EF, 0x006C, 0x006C, 0x006F});
  testStringToCodePoints("hüóOO", {0x0068, 0x00FC, 0x00F3, 0x004F, 0x004F});
  testStringToCodePoints("\u840C", {0x840C});

  VELOX_ASSERT_THROW(
      testStringToCodePoints("\xA9", {}),
      "Invalid UTF-8 encoding in characters");
  VELOX_ASSERT_THROW(
      testStringToCodePoints("ü\xA9", {}),
      "Invalid UTF-8 encoding in characters");
  VELOX_ASSERT_THROW(
      testStringToCodePoints("ü\xA9hello wooooorld", {}),
      "Invalid UTF-8 encoding in characters");
  VELOX_ASSERT_THROW(
      testStringToCodePoints("ü\xA9hello wooooooooorrrrrld", {}),
      "Invalid UTF-8 encoding in characters");
}

TEST_F(StringImplTest, overlappedStringPosition) {
  auto testValidInputAsciiLpos = [](std::string_view string,
                                    std::string_view substr,
                                    const int64_t instance,
                                    const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ true, true>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };
  auto testValidInputAsciiRpos = [](std::string_view string,
                                    std::string_view substr,
                                    const int64_t instance,
                                    const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ true, false>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };

  auto testValidInputUnicodeLpos = [](std::string_view string,
                                      std::string_view substr,
                                      const int64_t instance,
                                      const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ false, true>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };

  auto testValidInputUnicodeRpos = [](std::string_view string,
                                      std::string_view substr,
                                      const int64_t instance,
                                      const int64_t expectedPosition) {
    auto result =
        stringPosition</*isAscii*/ false, false>(string, substr, instance);
    ASSERT_EQ(result, expectedPosition);
  };

  testValidInputAsciiLpos("aaa", "aa", 2, 2L);
  testValidInputAsciiRpos("aaa", "aa", 2, 1L);

  testValidInputAsciiLpos("|||", "||", 2, 2L);
  testValidInputAsciiRpos("|||", "||", 2, 1L);

  testValidInputUnicodeLpos("😋😋😋", "😋😋", 2, 2L);
  testValidInputUnicodeRpos("😋😋😋", "😋😋", 2, 1L);

  testValidInputUnicodeLpos("你你你", "你你", 2, 2L);
  testValidInputUnicodeRpos("你你你", "你你", 2, 1L);
}

TEST_F(StringImplTest, stringPosition) {
  auto testValidInputAscii = [](std::string_view string,
                                std::string_view substr,
                                const int64_t instance,
                                const int64_t expectedPosition) {
    ASSERT_EQ(
        stringPosition</*isAscii*/ true>(string, substr, instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(string, substr, instance),
        expectedPosition);
  };

  auto testValidInputUnicode = [](std::string_view string,
                                  std::string_view substr,
                                  const int64_t instance,
                                  const int64_t expectedPosition) {
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(string, substr, instance),
        expectedPosition);
    ASSERT_EQ(
        stringPosition</*isAscii*/ false>(string, substr, instance),
        expectedPosition);
  };

  testValidInputAscii("high", "ig", 1, 2L);
  testValidInputAscii("high", "igx", 1, 0L);
  testValidInputAscii("Quadratically", "a", 1, 3L);
  testValidInputAscii("foobar", "foobar", 1, 1L);
  testValidInputAscii("foobar", "obar", 1, 3L);
  testValidInputAscii("zoo!", "!", 1, 4L);
  testValidInputAscii("x", "", 1, 1L);
  testValidInputAscii("", "", 1, 1L);
  testValidInputAscii("abc/xyz/foo/bar", "/", 3, 12L);

  testValidInputUnicode("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u7231", 1, 4L);
  testValidInputUnicode(
      "\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "\u5E0C\u671B", 1, 6L);
  testValidInputUnicode("\u4FE1\u5FF5,\u7231,\u5E0C\u671B", "nice", 1, 0L);

  testValidInputUnicode("abc/xyz/foo/bar", "/", 1, 4L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 2, 8L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 3, 12L);
  testValidInputUnicode("abc/xyz/foo/bar", "/", 4, 0L);

  EXPECT_THROW(
      stringPosition</*isAscii*/ false>("foobar", "foobar", 0), VeloxUserError);
}

TEST_F(StringImplTest, replaceFirst) {
  auto runTest = [](const std::string& string,
                    const std::string& replaced,
                    const std::string& replacement,
                    const std::string& expectedResults) {
    // Test out of place
    core::StringWriter output;
    replace(
        output,
        StringView(string),
        StringView(replaced),
        StringView(replacement),
        true);

    ASSERT_EQ(
        StringView(output.data(), output.size()), StringView(expectedResults));

    // Test in place
    if (replacement.size() <= replaced.size()) {
      core::StringWriter inOutString;
      inOutString.resize(string.size());
      if (string.size()) {
        std::memcpy(inOutString.data(), string.data(), string.size());
      }

      replaceInPlace(
          inOutString, StringView(replaced), StringView(replacement), true);
      ASSERT_EQ(
          StringView(inOutString.data(), inOutString.size()),
          StringView(expectedResults));
    }
  };

  runTest("hello_world", "e", "test", "htestllo_world");
  runTest("hello_world", "l", "test", "hetestlo_world");
  runTest("hello_world", "_", "", "helloworld");
  runTest("hello_world", "hello", "", "_world");
  runTest("aaa", "a", "b", "baa");
  runTest("replace_all", "all", "first", "replace_first");
  runTest(
      "The quick brown dog jumps over a lazy dog",
      "dog",
      "fox",
      "The quick brown fox jumps over a lazy dog");
  runTest("John  Doe", " ", "", "John Doe");
  runTest(
      "We will fight for our rights, for our rights.",
      ", for our rights",
      "",
      "We will fight for our rights.");
  runTest("Testcases test cases", "cases", "", "Test test cases");
  runTest("test cases", "", "Add ", "Add test cases");
  runTest("test cases", "not_found", "Add ", "test cases");
  runTest("", "a", "b", "");
  runTest("", "", "test", "test");
  runTest("", "a", ")", "");

  // Unicode tests
  runTest(
      "\u4FE1\u5FF5,\u7231,\u5E0C\u671B",
      ",",
      "\u2014",
      "\u4FE1\u5FF5\u2014\u7231,\u5E0C\u671B");
  runTest("\u00D6_hello_world", "", "prepend", "prepend\u00D6_hello_world");
}

TEST_F(StringImplTest, replace) {
  auto runTest = [](const std::string& string,
                    const std::string& replaced,
                    const std::string& replacement,
                    const std::string& expectedResults) {
    // Test out of place
    core::StringWriter output;
    replace(
        output,
        StringView(string),
        StringView(replaced),
        StringView(replacement));

    ASSERT_EQ(
        StringView(output.data(), output.size()), StringView(expectedResults));

    // Test in place
    if (replacement.size() <= replaced.size()) {
      core::StringWriter inOutString;
      inOutString.resize(string.size());
      if (string.size()) {
        std::memcpy(inOutString.data(), string.data(), string.size());
      }

      replaceInPlace(
          inOutString, StringView(replaced), StringView(replacement));
      ASSERT_EQ(
          StringView(inOutString.data(), inOutString.size()),
          StringView(expectedResults));
    }
  };

  runTest("aaa", "a", "aa", "aaaaaa");
  runTest("abcdefabcdef", "cd", "XX", "abXXefabXXef");
  runTest("abcdefabcdef", "cd", "", "abefabef");
  runTest("123123tech", "123", "", "tech");
  runTest("123tech123", "123", "", "tech");
  runTest("222tech", "2", "3", "333tech");
  runTest("0000123", "0", "", "123");
  runTest("0000123", "0", " ", "    123");
  runTest("foo", "", "", "foo");
  runTest("foo", "foo", "", "");
  runTest("abc", "", "xx", "xxaxxbxxcxx");
  runTest("", "", "xx", "xx");
  runTest("", "", "", "");

  runTest(
      "\u4FE1\u5FF5,\u7231,\u5E0C\u671B",
      ",",
      "\u2014",
      "\u4FE1\u5FF5\u2014\u7231\u2014\u5E0C\u671B");
  runTest("\u00D6sterreich", "\u00D6", "Oe", "Oesterreich");
}

TEST_F(StringImplTest, getByteRange) {
  // Unicode string
  char* unicodeString = (char*)"\uFE3D\uFE4B\uFF05abc";

  // Number of characters
  int unicodeStringCharacters = 6;

  // Size of all its prefixes
  std::array<const char*, 7> unicodeStringPrefixes{
      "", // dummy
      "",
      "\uFE3D",
      "\uFE3D\uFE4B",
      "\uFE3D\uFE4B\uFF05",
      "\uFE3D\uFE4B\uFF05a",
      "\uFE3D\uFE4B\uFF05ab",
  };

  // Locations precomputed in bytes
  std::vector<int> locationInBytes(7);
  for (int i = 1; i <= unicodeStringCharacters; i++) {
    locationInBytes[i] = strlen(unicodeStringPrefixes[i]);
  }

  // Test getByteRange
  for (int i = 1; i <= unicodeStringCharacters; i++) {
    auto expectedStartByteIndex = locationInBytes[i];
    auto expectedEndByteIndex = strlen(unicodeString);

    // Find the byte range of unicodeString[i, end]
    auto range =
        getByteRange</*isAscii*/ false>(unicodeString, 12, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);

    range = getByteRange</*isAscii*/ false>(unicodeString, 12, i, 6 - i + 1);

    EXPECT_EQ(expectedStartByteIndex, range.first);
    EXPECT_EQ(expectedEndByteIndex, range.second);
  }

  // Test bad unicode strings.

  // This exercises bad unicode byte in determining startByteIndex.
  std::string badUnicode = "aa\xff  ";
  auto range =
      getByteRange<false>(badUnicode.data(), badUnicode.length(), 4, 2);
  EXPECT_EQ(range.first, 3);
  EXPECT_EQ(range.second, 5);

  // This exercises bad unicode byte in determining endByteIndex.
  badUnicode = "\xff aa";
  range = getByteRange<false>(badUnicode.data(), badUnicode.length(), 1, 3);
  EXPECT_EQ(range.first, 0);
  EXPECT_EQ(range.second, 3);
}

TEST_F(StringImplTest, pad) {
  auto runTest = [](const std::string& string,
                    const int64_t size,
                    const std::string& padString,
                    const std::string& expectedLpadResult,
                    const std::string& expectedRpadResult) {
    core::StringWriter lpadOutput;
    core::StringWriter rpadOutput;

    bool stringIsAscii = isAscii(string.c_str(), string.size());
    bool padStringIsAscii = isAscii(padString.c_str(), padString.size());
    if (stringIsAscii && padStringIsAscii) {
      facebook::velox::functions::stringImpl::
          pad<true /*lpad*/, true /*isAscii*/>(
              lpadOutput, StringView(string), size, StringView(padString));
      facebook::velox::functions::stringImpl::
          pad<false /*lpad*/, true /*isAscii*/>(
              rpadOutput, StringView(string), size, StringView(padString));
    } else {
      // At least one of the string args is non-ASCII
      facebook::velox::functions::stringImpl::
          pad<true /*lpad*/, false /*IsAscii*/>(
              lpadOutput, StringView(string), size, StringView(padString));
      facebook::velox::functions::stringImpl::
          pad<false /*lpad*/, false /*IsAscii*/>(
              rpadOutput, StringView(string), size, StringView(padString));
    }

    ASSERT_EQ(
        StringView(lpadOutput.data(), lpadOutput.size()),
        StringView(expectedLpadResult));
    ASSERT_EQ(
        StringView(rpadOutput.data(), rpadOutput.size()),
        StringView(expectedRpadResult));
  };

  auto runTestUserError = [](const std::string& string,
                             const int64_t size,
                             const std::string& padString) {
    core::StringWriter output;

    bool padStringIsAscii = isAscii(padString.c_str(), padString.size());
    if (padStringIsAscii) {
      EXPECT_THROW(
          (facebook::velox::functions::stringImpl::pad<true, true>(
              output, StringView(string), size, StringView(padString))),
          VeloxUserError);
    } else {
      EXPECT_THROW(
          (facebook::velox::functions::stringImpl::pad<true, false>(
              output, StringView(string), size, StringView(padString))),
          VeloxUserError);
    }
  };

  // ASCII string with various values for size and padString
  runTest("text", 5, "x", "xtext", "textx");
  runTest("text", 4, "x", "text", "text");
  runTest("text", 6, "xy", "xytext", "textxy");
  runTest("text", 7, "xy", "xyxtext", "textxyx");
  runTest("text", 9, "xyz", "xyzxytext", "textxyzxy");
  // Non-ASCII string with various values for size and padString
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      10,
      "\u671B",
      "\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      11,
      "\u671B",
      "\u671B\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u671B\u671B");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      12,
      "\u5E0C\u671B",
      "\u5E0C\u671B\u5E0C\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      13,
      "\u5E0C\u671B",
      "\u5E0C\u671B\u5E0C\u671B\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  \u5E0C\u671B\u5E0C\u671B");
  // Empty string
  runTest("", 3, "a", "aaa", "aaa");
  // Truncating string
  runTest("abc", 0, "e", "", "");
  runTest("text", 3, "xy", "tex", "tex");
  runTest(
      "\u4FE1\u5FF5 \u7231 \u5E0C\u671B  ",
      5,
      "\u671B",
      "\u4FE1\u5FF5 \u7231 ",
      "\u4FE1\u5FF5 \u7231 ");

  // Empty padString
  runTestUserError("text", 10, "");
  // size outside the allowed range
  runTestUserError("text", -1, "a");
  runTestUserError(
      "text", ((int64_t)std::numeric_limits<int32_t>::max()) + 1, "a");
  // Additional tests with bad unicode bytes.
  runTest("abcd\xff \xff ef", 6, "0", "abcd\xff ", "abcd\xff ");
  runTest(
      "abcd\xff \xff ef", 11, "0", "0abcd\xff \xff ef", "abcd\xff \xff ef0");
  runTest("abcd\xff ef", 6, "0", "abcd\xff ", "abcd\xff ");
  // Testcase for when padString is a sequence of unicode continuation bytes
  // for which effective length is 0.
  runTestUserError(/*string=*/"\u4FE1", /*size=*/6, /*padString=*/"\xBF\xBF");
}

// Make sure that utf8proc_codepoint returns invalid codepoint (-1) for
// incomplete character of length>1.
TEST_F(StringImplTest, utf8proc_codepoint) {
  int size;

  std::string twoBytesChar = "\xdd\x81";
  EXPECT_EQ(
      utf8proc_codepoint(twoBytesChar.data(), twoBytesChar.data() + 1, size),
      -1);
  EXPECT_NE(
      utf8proc_codepoint(twoBytesChar.data(), twoBytesChar.data() + 2, size),
      -1);
  EXPECT_EQ(size, 2);

  std::string threeBytesChar = "\xe0\xa4\x86";
  for (int i = 1; i <= 2; i++) {
    EXPECT_EQ(
        utf8proc_codepoint(
            threeBytesChar.data(), threeBytesChar.data() + i, size),
        -1);
  }

  EXPECT_NE(
      utf8proc_codepoint(
          threeBytesChar.data(), threeBytesChar.data() + 3, size),
      -1);
  EXPECT_EQ(size, 3);

  std::string fourBytesChar = "\xf0\x92\x80\x85";
  for (int i = 1; i <= 3; i++) {
    EXPECT_EQ(
        utf8proc_codepoint(
            fourBytesChar.data(), fourBytesChar.data() + i, size),
        -1);
  }
  EXPECT_NE(
      utf8proc_codepoint(fourBytesChar.data(), fourBytesChar.data() + 4, size),
      -1);
  EXPECT_EQ(size, 4);
}

TEST_F(StringImplTest, isUnicodeWhiteSpace) {
  EXPECT_FALSE(isUnicodeWhiteSpace(-1));
}

TEST_F(StringImplTest, isAscii) {
  std::string s(101, 'a');
  ASSERT_TRUE(isAscii(s.data(), 1));
  ASSERT_TRUE(isAscii(s.data(), s.size()));
  const char* alpha = "\u03b1";
  memcpy(&s[0], alpha, strlen(alpha));
  ASSERT_FALSE(isAscii(s.data(), strlen(alpha)));
  ASSERT_FALSE(isAscii(s.data(), s.size()));
}

TEST_F(StringImplTest, initcapUnicodePresto) {
  for (const auto& [input, expected] : getInitcapUnicodePrestoTestData()) {
    std::string output;
    initcap<
        /*strictSpace=*/false,
        /*isAscii=*/false,
        /*turkishCasing=*/false,
        /*greekFinalSigma=*/false>(output, input);
    ASSERT_EQ(output, expected);
  }
}

TEST_F(StringImplTest, initcapAsciiPresto) {
  for (const auto& [input, expected] : getInitcapAsciiPrestoTestData()) {
    std::string output;
    initcap<
        /*strictSpace=*/false,
        /*isAscii=*/true,
        /*turkishCasing=*/false,
        /*greekFinalSigma=*/false>(output, input);
    ASSERT_EQ(output, expected);
  }
}
TEST_F(StringImplTest, initcapUnicodeSpark) {
  for (const auto& [input, expected] : getInitcapUnicodeSparkTestData()) {
    std::string output;
    initcap<
        /*strictSpace=*/true,
        /*isAscii=*/false,
        /*turkishCasing=*/true,
        /*greekFinalSigma=*/true>(output, input);
    ASSERT_EQ(output, expected);
  }
}

TEST_F(StringImplTest, initcapAsciiSpark) {
  for (const auto& [input, expected] : getInitcapAsciiSparkTestData()) {
    std::string output;
    initcap<
        /*strictSpace=*/true,
        /*isAscii=*/true,
        /*turkishCasing=*/true,
        /*greekFinalSigma=*/true>(output, input);
    ASSERT_EQ(output, expected);
  }
}
