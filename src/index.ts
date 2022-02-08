import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';

/**
 * Initialization data for the dps-jupyter-server-extension extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'dps-jupyter-server-extension:plugin',
  autoStart: true,
  activate: (app: JupyterFrontEnd) => {
    console.log('JupyterLab extension dps-jupyter-server-extension is activated!');
  }
};

export default plugin;
