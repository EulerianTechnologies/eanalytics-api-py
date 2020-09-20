import ipywidgets as _widgets
# https://ipywidgets.readthedocs.io/en/latest/examples/Widget%20List.html

def dropdown(
    d : dict,
    label = 'key',
    **kwargs,
):
    """ Display a dropdown for a given dictionnary

        Parameters
        ----------
        d : dict, mandatory,

        **kwargs: arguments for widgets.Dropdown

        label: str, optional, allowed='key', 'value'
            display key as label or value as label
            default: 'key'

    Returns
    -------
    HTML dropdown to toggle cell visibility.
    """
    if not isinstance(d, dict):
        raise TypeError("Argument should be a dictionnary")

    if label == 'key':
        list_of_tuples = [(k, v) for k, v in d.items()]
    elif label == 'value':
        list_of_tuples = [(v, k) for k, v in d.items()]
    else:
        raise ValueError("Allowed labels=key, value")

    return _widgets.Dropdown(
        options=list_of_tuples,
        **kwargs
    )