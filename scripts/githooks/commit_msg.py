#!/usr/bin/env python

# Check a Git commit message according to the seven rules of a good commit message:
# https://chris.beams.io/posts/git-commit/
import sys

class GitCommitMessage:
    'Represents a parsed Git commit message'

    rules = [ 'Separate subject from body with a blank line',
              'Limit the subject line to 50 characters',
              'Capitalize the subject line',
              'Do not end the subject line with a period',
              'Use the imperative mood in the subject line',
              'Wrap the body at 72 characters',
              'Use the body to explain what and why vs. how' ]

    valid_rules = [ False, False, False, False, False, False, False ]

    def __init__(self, filename = None):
        lines = []

        if filename != None:
            with open(filename, 'r') as f:
                for line in f:
                    if line.startswith('# ------------------------ >8 ------------------------'):
                        break
                    if not line.startswith('#'):
                        lines.append(line)

        self.parse_lines(lines)

    def parse_lines(self, lines):
        self.body_lines = []
        self.subject = []

        if not lines or len(lines) == 0:
            return self

        self.subject = lines[0]
        self.subject_words = self.subject.split()
        self.has_subject_body_separator = False

        if len(lines) > 1:
            self.has_subject_body_separator = (len(lines[1].strip()) == 0)

            if self.has_subject_body_separator:
                self.body_lines = lines[2:]
            else:
                self.body_lines = lines[1:]

        return self

    def check_subject_body_separtor(self):
        'Rule 1: Separate subject from body with a blank line'

        if len(self.body_lines) > 0:
            return self.has_subject_body_separator
        return True

    def check_subject_limit(self):
        'Rule 2: Limit the subject line to 50 characters'
        return len(self.subject) <= 50

    def check_subject_capitalized(self):
        'Rule 3: Capitalize the subject line'
        return len(self.subject) > 0 and self.subject[0].isupper()

    def check_subject_no_period(self):
        'Rule 4: Do not end the subject line with a period'
        return not self.subject.endswith('.')

    common_first_words = [ 'Add',
                           'Adjust',
                           'Support',
                           'Change',
                           'Remove',
                           'Fix',
                           'Print',
                           'Track',
                           'Refactor',
                           'Combine',
                           'Release',
                           'Set',
                           'Stop',
                           'Make',
                           'Mark',
                           'Enable',
                           'Check',
                           'Exclude',
                           'Format',
                           'Correct']

    def check_subject_imperative(self):
        'Rule 5: Use the imperative mood in the subject line.'
        'We can only check for common mistakes here, like using'
        'the -ing form of a verb or non-imperative version of '
        'common verbs'

        firstword = self.subject_words[0]

        if firstword.endswith('ing'):
            return False

        for word in self.common_first_words:
            if firstword.startswith(word) and firstword != word:
                return False

        return True

    def check_body_limit(self):
        'Rule 6: Wrap the body at 72 characters'

        if len(self.body_lines) == 0:
            return True

        for line in self.body_lines:
            if len(line) > 72:
                return False

        return True

    def check_body_uses_why(self):
        'Rule 7: Use the body to explain what and why vs. how'
        # Not enforcable
        return True

    rule_funcs = [
        check_subject_body_separtor,
        check_subject_limit,
        check_subject_capitalized,
        check_subject_no_period,
        check_subject_imperative,
        check_body_limit,
        check_body_uses_why ]

    def check_the_seven_rules(self):
        'validates the commit message against the seven rules'

        num_violations = 0

        for i in range(len(self.rule_funcs)):
            func = self.rule_funcs[i]
            res = func(self)
            self.valid_rules[i] = res

            if not res:
                num_violations += 1

        if num_violations > 0:
            print
            print("**** WARNING ****")
            print
            print("The commit message does not seem to comply with the project's guidelines.")
            print("Please try to follow the \"Seven rules of a great commit message\":")
            print("https://chris.beams.io/posts/git-commit/")
            print
            print("The following rules are violated:\n")

            for i in range(len(self.rule_funcs)):
                if not self.valid_rules[i]:
                    print("\t* Rule %d: \"%s\"" % (i+1, self.rules[i]))

        # Extra sanity checks beyond the seven rules
        if len(self.body_lines) == 0:
            print
            print("NOTE: the commit message has no body.")
            print("It is recommended to add a body with a description of your")
            print("changes, even if they are small. Explain what and why instead of how:")
            print("https://chris.beams.io/posts/git-commit/#why-not-how")

        if len(self.subject_words) < 3:
            print
            print("Warning: the subject line has less than three words.")
            print("Consider using a more explanatory subject line.")

        if num_violations > 0:
            print
            print("Run 'git commit --amend' to change the commit message")

        print

        return num_violations

def main():
    if len(sys.argv) != 2:
        print("Unexpected number of arguments")
        exit(1)

    msg = GitCommitMessage(sys.argv[1])
    return msg.check_the_seven_rules()

if __name__ == "__main__":
    main()
    # Always exit with success. We could also fail the commit if with
    # a non-zero exit code, but that might be a bit excessive and we'd
    # have to save the failed commit message to a file so that it can
    # be recovered.
    exit(0)
