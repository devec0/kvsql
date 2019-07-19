package driver

import "time"

func retry(f func() error) error {
	var err error
	for i := 0; i < 10; i++ {
		if err = f(); err != nil {
			if err.Error() == "database is locked" {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			return err
		}
		break
	}
	return err
}
