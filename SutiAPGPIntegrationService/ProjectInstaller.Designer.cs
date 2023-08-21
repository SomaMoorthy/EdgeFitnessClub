namespace SUTIAPGPIntegrationService
{
    partial class ProjectInstaller
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary> 
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Component Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.SUTIAPGPIntegrationServiceProcessInstaller = new System.ServiceProcess.ServiceProcessInstaller();
            this.SUTIAPGPIntegrationServiceInstaller = new System.ServiceProcess.ServiceInstaller();
            // 
            // SUTIAPGPIntegrationServiceProcessInstaller
            // 


            this.SUTIAPGPIntegrationServiceProcessInstaller.Account = System.ServiceProcess.ServiceAccount.LocalSystem;
            this.SUTIAPGPIntegrationServiceProcessInstaller.Password = null;
            this.SUTIAPGPIntegrationServiceProcessInstaller.Username = null;
            // 
            // SUTIAPGPIntegrationServiceInstaller
            // 
            this.SUTIAPGPIntegrationServiceInstaller.Description = "SUTI AP GP IntegrationService";
            this.SUTIAPGPIntegrationServiceInstaller.DisplayName = "SUTIAPGPIntegrationService"; 
            this.SUTIAPGPIntegrationServiceInstaller.ServiceName = "SUTIAPGPIntegrationService";
            // 
            // ProjectInstaller
            // 
            this.Installers.AddRange(new System.Configuration.Install.Installer[] {
            this.SUTIAPGPIntegrationServiceProcessInstaller,
            this.SUTIAPGPIntegrationServiceInstaller});

        }

        #endregion

        private System.ServiceProcess.ServiceProcessInstaller SUTIAPGPIntegrationServiceProcessInstaller;
        private System.ServiceProcess.ServiceInstaller SUTIAPGPIntegrationServiceInstaller;
    }
}