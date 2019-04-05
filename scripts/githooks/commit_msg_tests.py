#!/usr/bin/env python

import unittest
from commit_msg import GitCommitMessage

class TestCommitMsg(unittest.TestCase):

    def testNoInput(self):
        m = GitCommitMessage().parse_lines([])
        self.assertEqual(len(m.body_lines), 0)
        m = GitCommitMessage().parse_lines(None)
        self.assertEqual(len(m.body_lines), 0)

    def testParsing(self):
        m = GitCommitMessage().parse_lines(['This is a subject line', '   ', 'body line 1', 'body line 2'])
        self.assertEqual(m.subject, 'This is a subject line')
        self.assertTrue(m.has_subject_body_separator)
        self.assertEqual(m.body_lines[0], 'body line 1')
        self.assertEqual(m.body_lines[1], 'body line 2')

    def testNonImperative(self):
        m = GitCommitMessage().parse_lines(['Adds new file'])
        self.assertFalse(m.check_subject_imperative())

        m.parse_lines(['Adding new file'])
        self.assertFalse(m.check_subject_imperative())

        # Default to accept
        m.parse_lines(['Foo bar'])
        self.assertTrue(m.check_subject_imperative())

    def testSubjectBodySeparator(self):
        m = GitCommitMessage().parse_lines(['This is a subject line'])
        self.assertTrue(m.check_subject_body_separtor())
        m = GitCommitMessage().parse_lines(['This is a subject line', 'body'])
        self.assertFalse(m.check_subject_body_separtor())
        m = GitCommitMessage().parse_lines(['This is a subject line', '', 'body'])
        self.assertTrue(m.check_subject_body_separtor())
        m = GitCommitMessage().parse_lines(['This is a subject line', '   ', 'body'])
        self.assertTrue(m.check_subject_body_separtor())

    def testSubjectLimit(self):
        m = GitCommitMessage().parse_lines(['This subject line is exactly 48 characters long'])
        self.assertTrue(m.check_subject_limit())
        m = GitCommitMessage().parse_lines(['This is a very long subject line that will obviously exceed the limit'])
        self.assertFalse(m.check_subject_limit())

    def testSubjectCapitalized(self):
        m = GitCommitMessage().parse_lines(['This subject line is capitalized'])
        self.assertTrue(m.check_subject_capitalized())
        m = GitCommitMessage().parse_lines(['this subject line is not capitalized'])
        self.assertFalse(m.check_subject_capitalized())

    def testSubjectNoPeriod(self):
        m = GitCommitMessage().parse_lines(['This subject line ends with a period.'])
        self.assertFalse(m.check_subject_no_period())
        m = GitCommitMessage().parse_lines(['This subject line does not end with a period'])
        self.assertTrue(m.check_subject_no_period())

    def testBodyLimit(self):
        m = GitCommitMessage().parse_lines(['This is a subject line', ' ', 'A short body line'])
        self.assertTrue(m.check_body_limit())
        m = GitCommitMessage().parse_lines(['This is a subject line', ' ', 'A very long body line which certainly exceeds the 72 char recommended limit'])
        self.assertFalse(m.check_body_limit())

    def testCheckAllRules(self):
        m = GitCommitMessage().parse_lines(['This is a subject line', '', 'A short body line'])
        self.assertEqual(0, m.check_the_seven_rules())

if __name__ == "__main__":
    unittest.main()
