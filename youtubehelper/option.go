package youtubehelper

type options struct {
        verbose     bool
}

func defaultOptions() (*options){
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
