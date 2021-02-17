class TimeFilter:
    """ A collection of Time filtering functions. """

    @staticmethod
    def apply(log, value):
        """Apply the time filter.

        Args:
            log (pd.DataFrame):
                The transition log.
            value (dict):
                The filter's value or settings.

        Returns:
            pd.DataFrame:
                The filtered transition log.
        """
        modes = {
            'intersecting': TimeFilter.intersect,
            'contain': TimeFilter.contain,
            'completed_in': TimeFilter.completed_in,
            'started_in': TimeFilter.started_in,
            'trim': TimeFilter.trim,
        }

        mode = value.get('mode')
        if mode in modes:
            return modes.get(mode)(log, value.get('startTime'), value.get('endTime'))
        else:
            return 'Provide a valid mode please'

    @staticmethod
    def intersect(log, start_time, end_time):
        """ Filter the log based on an intersection.

        If the log has projects that intersect the start or end time they're included.

        Args:
            log (pd.DataFrame):
                The transition log.
            start_time (str):
                The filter's start time.
            end_time (str):
                The filter's end time.

        Returns:
            pd.DataFrame:
                The filtered transition log.
        """
        if (start_time and end_time):
            return log\
                .groupby(['WF_nr'])\
                .filter(lambda x: (x['time:timestamp'] >= start_time).any() & (x['time:timestamp'] <= end_time).any())

        print('Filter not applied')
        return log

    @staticmethod
    def contain(log, start_time, end_time):
        """ Filter the log projects within the timeframe.

        If the log has projects that are entirely within the start and end time they're included.

        Args:
            log (pd.DataFrame):
                The transition log.
            start_time (str):
                The filter's start time.
            end_time (str):
                The filter's end time.

        Returns:
            pd.DataFrame:
                The filtered transition log.
        """
        if (start_time and end_time):
            return log\
                .groupby(['WF_nr'])\
                .filter(lambda x: (x['time:timestamp'] >= start_time).all() & (x['time:timestamp'] <= end_time).all())

        print('Filter not applied')
        return log

    @staticmethod
    def completed_in(log, start_time, end_time):
        """ Filter the log based projects before end_time.

        If the log has projects that completed before the end time they're included.

        Args:
            log (pd.DataFrame):
                The transition log.
            start_time (str):
                The filter's start time.
            end_time (str):
                The filter's end time.

        Returns:
            pd.DataFrame:
                The filtered transition log.
        """
        if (start_time and end_time):
            return log\
                .groupby(['WF_nr'])\
                .filter(lambda x: (x['time:timestamp'] <= end_time).all() & (x['time:timestamp'] >= start_time).any())

        print('Filter not applied')
        return log

    @staticmethod
    def started_in(log, start_time, end_time):
        """ Filter the log based projects after start_time.

        If the log has projects that started after the start time they're included.

        Args:
            log (pd.DataFrame):
                The transition log.
            start_time (str):
                The filter's start time.
            end_time (str):
                The filter's end time.

        Returns:
            pd.DataFrame:
                The filtered transition log.
        """
        if (start_time and end_time):
            return log\
                .groupby(['WF_nr'])\
                .filter(lambda x: (x['time:timestamp'] >= start_time).all() & (x['time:timestamp'] <= end_time).any())

        print('Filter not applied')
        return log

    @staticmethod
    def trim(log, start_time, end_time):
        """ Filter the log and trim projects.

        If the log has projects that include data
        from before or after the filter's timeframe those *rows*
        will be excluded.

        Args:
            log (pd.DataFrame):
                The transition log.
            start_time (str):
                The filter's start time.
            end_time (str):
                The filter's end time.

        Returns:
            pd.DataFrame:
                The filtered transition log.
        """
        if (start_time and end_time):
            return log[(log['time:timestamp'] >= start_time) & (log['time:timestamp'] <= end_time)]

        print('Filter not applied')
        return log
