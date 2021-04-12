package counter

import (
	"github.com/shogo82148/go-mecab"
	"regexp"
	"strings"
	"github.com/tmdvs/Go-Emoji-Utils"
	"golang.org/x/text/unicode/norm"
)

type options struct {
	verbose bool
}

func defaultOptions() *options {
	return &options{
		verbose: false,
	}
}

type Option func(*options)

func Verbose(verbose bool) Option {
	return func(opts *options) {
		opts.verbose = verbose
	}
}

type WordCounter struct {
	verbose bool
	mecabrc string
	result  map[string]int
	splitRe *regexp.Regexp
	stampRe *regexp.Regexp
	emojiRe *regexp.Regexp
	symbolRe *regexp.Regexp
}

func (w *WordCounter) isAlphabets(s string) bool {
	for _, r := range s {
		if !((r >= ' ' && r <= '~') || r == '　' || r == '\t') {
			return false
		}
	}
	return true
}

func (w *WordCounter) addWords(words []string) {
	for _, word := range words {
		_, ok := w.result[word]
		if !ok {
			w.result[word] = 1
		} else {
			w.result[word] += 1
		}
	}
}

func (w *WordCounter) parseNonJapanease(text string) {
	words := w.splitRe.Split(text, -1)
	w.addWords(words)
}

func (w *WordCounter) parseJapanease(text string) {
	opts := make(map[string]string)
	opts["rcfile"] = w.mecabrc
	tagger, err := mecab.New(opts)
	if err != nil {
		return
	}
	defer tagger.Destroy()
	result, err := tagger.Parse(text)
	if err != nil {
		return
	}
	lines := strings.Split(result, "\n")
	morphs := make([]string, 0, len(text))
	kugiri := false
	words := make([]string, 0, len(text))
	for _, ln := range lines {
		es := strings.Split(ln, ",")
		wt := w.splitRe.Split(es[0], -1)
		if len(wt) < 2 {
			word := strings.Join(morphs, "")
			words = append(words, word)
			break
		}
		if wt[1] == "助詞" || wt[1] == "記号" || wt[1] == "特殊" {
			kugiri = true
		} else if wt[1] != "助詞" && wt[1] != "記号" && wt[1] != "特殊" && kugiri == true {
			word := strings.Join(morphs, "")
			words = append(words, word)
			kugiri = false
			morphs = make([]string, 0, len(text))
		}
		if wt[1] == "記号" || wt[1] == "特殊" {
			continue
		}
		morphs = append(morphs, wt[0])
	}
	w.addWords(words)
}

func (w *WordCounter) Count(text string) {
	text = emoji.RemoveAll(text)
	text = norm.NFKC.String(text)
        text = w.stampRe.ReplaceAllString(text, "")
        text = w.symbolRe.ReplaceAllString(text, "")
	if w.isAlphabets(text) {
		w.parseNonJapanease(text)
	} else {
		w.parseJapanease(text)
	}
}

func (w *WordCounter) Result() map[string]int {
	return w.result
}

func NewWordCounter(mecabrc string, opts ...Option) *WordCounter {
	baseOpts := defaultOptions()
	for _, opt := range opts {
		opt(baseOpts)
	}
	return &WordCounter{
		verbose: baseOpts.verbose,
		mecabrc: mecabrc,
		result:  make(map[string]int),
		splitRe: regexp.MustCompile(`[ 　\t]+`),
		stampRe: regexp.MustCompile(`:.+?:`),
		symbolRe: regexp.MustCompile(`[!！?？,、.。]`),
	}
}
