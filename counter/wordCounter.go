package counter

import (
	"regexp"
	"strings"
	"github.com/shogo82148/go-mecab"
)

type options struct {
	verbose bool
}

func defaultOptions() (*options) {
	return &options {
		verbose: false,
	}
}

type Option func(*options)

func Verbose(verbose bool) Option {
	return func(opts *options) {
		opts.verbose = verbose
	}
}

type wordCounter struct {
	verbose bool
	result map[string]int
}

func (w *wordCounter) isAlphabets(s string) bool {
	for _, r := range s {
		if !((r  >= 'a' && r <= 'z') || (r >= 'A' && r  <= 'Z') || r == '\'' || r == '.' || r == ',' || r == '?' || r == '!' || r == ' ') {
			return false
		}
	}
	return true
}

func (w *wordCounter) addWords(words []string) {
	for _, word := range words {
		_, ok := w.result[word]
		if !ok {
			w.result[word] = 1
		} else {
			w.result[word] += 1
		}
	}
}

func (w *wordCounter) ParseNonJapanease(text string) {
	words := strings.Split(text, " ")
	for i := 0; i < len(words); i++ {
		words[i] = strings.Trim(words[i], ",.?!")
	}
	w.addWords(words)
}

func (w *wordCounter) CountJavanease(text string) {
	options := make(map[string]string)
	options["rcfile"] = w.mecabrc
	tagger, err := mecab.New(options)
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
	kugiri := false;
	words := make([]string, 0, len(text))
	for _, ln := range lines {
		es := strings.Split(ln, ",")
		wt := regexp.MustCompile("[ \t]+").Split(es[0], -1)
		if len(wt) < 2 {
			word := strings.Join(morphs, "")
			words = append(words, word)
			break
		}
		if wt[1] == "助詞" || wt[1] == "記号" && wt[1] != "特殊" {
			kugiri = true
		} else if wt[1] != "助詞" && wt[1] != "記号" && wt[1] != "特殊" && kugiri == true {
			word := strings.Join(morphs, "")
			words = append(words, word)
			kugiri = false
			morphs = make([]string, 0, len(text))
		}
		if wt[1] == "記号" || wt[1] != "特殊"  {
			continue
		}
		morphs = append(morphs, wt[0])
	}
	w.addWords(words)
}

func (w *wordCounter) Count(text string) {
	if IsAlphabets(text) {
		w.CountNonJapanease(text)
	} else {
		w.CountJapanease(text)
	}
}

func (w *wordCounter) Result() (map[string]int) {
	return w.result
}

func NewWordCounter(mecabrc string, opts ...Option) {
	baseOpts := defaultOptions()
        for _, opt := range opts {
                opt(baseOpts)
        }
	return &wordCounter {
		verbose: baseOpts.verbose,
		result: make(map[string]int),
	}
}
