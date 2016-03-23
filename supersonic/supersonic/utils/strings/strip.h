// Copyright 2011 Google Inc. All Rights Reserved.
//
// #status: RECOMMENDED
// #category: operations on strings
// #summary: Functions for removing a defined part of a string.
//
// This file contains various functions for "stripping" aka removing various
// characters and substrings from a string. Much of the code in here is old and
// operates on C-style strings such as char* and const char*. Prefer the
// interfaces that take and return StringPiece and C++ string objects. See
// //strings/stringpiece_utils.h for similar functions that operate on
// StringPiece.

#ifndef STRINGS_STRIP_H_
#define STRINGS_STRIP_H_

#include <stddef.h>

#include <string>
namespace supersonic {using std::string; }

#include "supersonic/utils/strings/ascii_ctype.h"
#include "supersonic/utils/strings/stringpiece.h"

// Returns a copy of the input string 'str' with the given 'prefix' removed. If
// the prefix doesn't match, returns a copy of the original string.
//
// The "Try" version stores the stripped string in the 'result' out-param and
// returns true iff the prefix was found and removed. It is safe for 'result' to
// point back to the input string.
string StripPrefixString(StringPiece str, StringPiece prefix);
bool TryStripPrefixString(StringPiece str,
                          StringPiece prefix,
                          string* result);

// Returns a copy of the input string 'str' with the given 'suffix' removed. If
// the suffix doesn't match, returns a copy of the original string.
//
// The "Try" version stores the stripped string in the 'result' out-param and
// returns true iff the suffix was found and removed. It is safe for 'result' to
// point back to the input string.
string StripSuffixString(StringPiece str, StringPiece suffix);
bool TryStripSuffixString(StringPiece str, StringPiece suffix,
                          string* result);

// Replaces any of the characters in 'remove' with the character 'replacewith'.
//
// This is a very poorly named function. This function should be renamed to
// something like ReplaceCharacters().
void StripString(char* str, StringPiece remove, char replacewith);
void StripString(char* str, int len, StringPiece remove, char replacewith);
void StripString(string* s, StringPiece remove, char replacewith);
inline void StripString(char* str, char remove, char replacewith) {
  for (; *str; str++) {
    if (*str == remove)
      *str = replacewith;
  }
}

// Replaces runs of one or more 'dup_char' with a single occurrence, and returns
// the number of characters that were removed.
//
// Example:
//       StripDupCharacters("a//b/c//d", '/', 0) => "a/b/c/d"
int StripDupCharacters(string* s, char dup_char, int start_pos);

// Removes whitespace from both ends of the given string. This function has
// various overloads for passing the input string as a C-string + length,
// string, or StringPiece. If the caller is using NUL-terminated strings, it is
// the caller's responsibility to insert the NUL character at the end of the
// substring.
void StripWhiteSpace(const char** str, int* len);
inline void StripWhiteSpace(char** str, int* len) {
  // The "real" type for StripWhiteSpace is ForAll char types C, take
  // (C, int) as input and return (C, int) as output.  We're using the
  // cast here to assert that we can take a char*, even though the
  // function thinks it's assigning to const char*.
  StripWhiteSpace(const_cast<const char**>(str), len);
}
void StripWhiteSpace(string* str);
inline void StripWhiteSpace(StringPiece* str) {
  const char* data = str->data();
  int len = str->size();
  StripWhiteSpace(&data, &len);
  str->set(data, len);
}

namespace strings {

// Calls StripWhiteSpace() on each element in the given collection.
//
// Note: this implementation is conceptually similar to
//
//   std::for_each(c.begin(), c.end(), StripWhiteSpace);
//
// except that StripWhiteSpace requires a *pointer* to the element, so the above
// std::for_each solution wouldn't work.
template <typename Collection>
inline void StripWhiteSpaceInCollection(Collection* collection) {
  for (typename Collection::iterator it = collection->begin();
       it != collection->end(); ++it)
    StripWhiteSpace(&(*it));
}

}  // namespace strings

// Removes whitespace from the beginning of the given string. The version that
// takes a string modifies the given input string.
//
// The versions that take C-strings return a pointer to the first non-whitespace
// character if one is present or NULL otherwise. 'line' must be NUL-terminated.
void StripLeadingWhiteSpace(string* str);
inline const char* StripLeadingWhiteSpace(const char* line) {
  // skip leading whitespace
  while (ascii_isspace(*line))
    ++line;

  if ('\0' == *line)  // end of line, no non-whitespace
    return NULL;

  return line;
}
// StripLeadingWhiteSpace for non-const strings.
inline char* StripLeadingWhiteSpace(char* line) {
  return const_cast<char*>(
      StripLeadingWhiteSpace(const_cast<const char*>(line)));
}

// Removes whitespace from the end of the given string.
void StripTrailingWhitespace(string* s);

// Removes the trailing '\n' or '\r\n' from 's', if one exists. Returns true if
// a newline was found and removed.
bool StripTrailingNewline(string* s);

// Removes leading, trailing, and duplicate internal whitespace.
void RemoveExtraWhitespace(string* s);

// Returns a pointer to the first non-whitespace character in 'str'. Never
// returns NULL. 'str' must be NUL-terminated.
inline const char* SkipLeadingWhiteSpace(const char* str) {
  while (ascii_isspace(*str))
    ++str;
  return str;
}
inline char* SkipLeadingWhiteSpace(char* str) {
  while (ascii_isspace(*str))
    ++str;
  return str;
}

// Strips everything enclosed in pairs of curly braces ('{' and '}') and the
// curly braces themselves. Doesn't touch open braces without a closing brace.
// Does not handle nesting.
void StripCurlyBraces(string* s);

// Performs the same operation as StripCurlyBraces, but allows the caller to
// specify different left and right bracket characters, such as '(' and ')'.
void StripBrackets(char left, char right, string* s);

// Strips everything between a right angle bracket ('<') and left angle bracket
// ('>') including the brackets themselves, e.g.
// "the quick <b>brown</b> fox" --> "the quick brown fox".
//
// This does not understand HTML nor does it know anything about HTML tags or
// comments. This is simply a text processing function that removes text between
// pairs of angle brackets. Note that in the example above the word "brown" is
// not removed because it is not between pairs of angle brackets.
//
// This is NOT safe for security and this will NOT prevent against XSS.
//
// For a more full-featured HTML parser, see //webutil/pageutil/pageutil.h.
void StripMarkupTags(string* s);
string OutputWithMarkupTagsStripped(const string& s);

// Removes any occurrences of the characters in 'remove' from the:
//
//   - start of the string "Left"
//   - end of the string "Right"
//   - both ends of the string
//
// Returns the number of chars removed.
int TrimStringLeft(string* s, StringPiece remove);
int TrimStringRight(string* s, StringPiece remove);
inline int TrimString(string* s, StringPiece remove) {
  return TrimStringRight(s, remove) + TrimStringLeft(s, remove);
}

// Removes leading and trailing runs, and collapses middle runs of a set of
// characters into a single character (the first one specified in 'remove').
// E.g.: TrimRunsInString(&s, " :,()") removes leading and trailing delimiter
// chars and collapses and converts internal runs of delimiters to single ' '
// characters, so, for example, "  a:(b):c  " -> "a b c".
void TrimRunsInString(string* s, StringPiece remove);

// Removes all internal '\0' characters from the string.
void RemoveNullsInString(string* s);

// Removes all occurrences of the given character from the given string. Returns
// the new length.
int strrm(char* str, char c);
int memrm(char* str, int strlen, char c);

// Removes all occurrences of any character from 'chars' from the given string.
// Returns the new length.
int strrmm(char* str, const char* chars);
int strrmm(string* str, const string& chars);

#endif  // STRINGS_STRIP_H_
